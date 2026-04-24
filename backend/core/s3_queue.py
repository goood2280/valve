"""Valve · s3_queue
---------------------
S3 업로드 지연 큐. immediate 이외 모드(interval / manual) 에서 사용.

설계:
- 큐는 메모리 + jsonl(logs/s3_queue.jsonl) append-only 로 복제
- interval 모드: 백그라운드 asyncio 루프가 `s3.upload_interval_sec` 간격으로 flush
- manual 모드: /api/jobs/s3-flush 만 flush 트리거
- 실패 시: `s3.retry_failed_sec` 후 다시 시도 (지수 backoff 없이 단순 재시도)
- 성공한 항목은 큐에서 제거 + 파티션 상태 'success' 로 업데이트
"""
from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Callable, Optional

_queue: list[dict] = []          # [{partition_key, local_path, s3_key, enqueued_at, attempts, last_error}]
_log_path: Path = None
_s3 = None
_settings = None
_state = None
_alert_cb: Optional[Callable[[dict], None]] = None
_bg_task: Optional[asyncio.Task] = None


def configure(s3, settings, state, log_path: Path, alert_cb=None):
    global _s3, _settings, _state, _log_path, _alert_cb
    _s3 = s3
    _settings = settings
    _state = state
    _log_path = Path(log_path)
    _log_path.parent.mkdir(parents=True, exist_ok=True)
    _alert_cb = alert_cb
    _replay()


def _replay():
    """재기동 시 큐 복원."""
    _queue.clear()
    if not _log_path or not _log_path.exists():
        return
    try:
        with open(_log_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line: continue
                try:
                    evt = json.loads(line)
                    kind = evt.get("kind")
                    if kind == "enqueue":
                        _queue.append(evt["item"])
                    elif kind == "complete":
                        pk = evt.get("partition_key")
                        _queue[:] = [q for q in _queue if q.get("partition_key") != pk]
                except Exception:
                    continue
    except Exception:
        pass


def _append(evt: dict):
    try:
        with open(_log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(evt, ensure_ascii=False, default=str) + "\n")
    except Exception:
        pass


def enqueue(partition_key: str, local_path: str, s3_key: str, mode: str = "interval"):
    """업로드 대기 큐에 추가. 동일 partition_key 가 이미 있으면 교체."""
    item = {
        "partition_key": partition_key,
        "local_path": local_path,
        "s3_key": s3_key,
        "enqueued_at": time.time(),
        "attempts": 0,
        "mode": mode,
    }
    _queue[:] = [q for q in _queue if q.get("partition_key") != partition_key]
    _queue.append(item)
    _append({"ts": time.time(), "kind": "enqueue", "item": item})


def pending() -> list[dict]:
    return list(_queue)


async def flush_once() -> dict:
    """대기 큐의 모든 항목을 한 번 업로드 시도. retry_failed_sec 이내 재시도는 건너뜀.
    반환: 실행 요약 카운트."""
    if not _s3:
        return {"ok": False, "error": "s3 not configured"}
    retry_sec = float(((_settings or {}).get("s3") or {}).get("retry_failed_sec") or 120)
    now = time.time()
    up, fail, skip = 0, 0, 0

    # 스냅샷 — 이터레이션 중 변경 방지
    items = list(_queue)
    for item in items:
        if item.get("attempts", 0) > 0 and (now - item.get("last_try_ts", 0)) < retry_sec:
            skip += 1
            continue
        local_path = Path(item["local_path"])
        if not local_path.exists():
            # 파일 이미 없으면 큐에서 제거
            _queue[:] = [q for q in _queue if q.get("partition_key") != item["partition_key"]]
            _append({"ts": time.time(), "kind": "complete", "partition_key": item["partition_key"],
                     "reason": "local_missing"})
            skip += 1
            continue
        try:
            await _s3.put_atomic(local_path, item["s3_key"])
            up += 1
            _queue[:] = [q for q in _queue if q.get("partition_key") != item["partition_key"]]
            _append({"ts": time.time(), "kind": "complete",
                     "partition_key": item["partition_key"], "s3_key": item["s3_key"]})
            if _state is not None:
                _state.update_partition(item["partition_key"], {"status": "success"})
        except Exception as e:
            fail += 1
            item["attempts"] = int(item.get("attempts", 0)) + 1
            item["last_error"] = str(e)[:300]
            item["last_try_ts"] = time.time()
            _append({"ts": time.time(), "kind": "fail_attempt",
                     "partition_key": item["partition_key"],
                     "attempts": item["attempts"], "error": item["last_error"]})
            if _alert_cb and item["attempts"] >= 3:
                try:
                    _alert_cb({
                        "source": "valve.s3_queue",
                        "kind": "s3_upload_retry_exhausted",
                        "severity": "error",
                        "title": f"S3 업로드 3회 실패: {item['partition_key']}",
                        "partition_key": item["partition_key"],
                        "s3_key": item["s3_key"],
                        "error": item["last_error"],
                    })
                except Exception:
                    pass
    return {"ok": True, "uploaded": up, "failed": fail, "skipped": skip,
            "pending": len(_queue)}


async def _loop():
    """interval 모드 백그라운드 루프. upload_interval_sec 간격."""
    while True:
        interval = float(((_settings or {}).get("s3") or {}).get("upload_interval_sec") or 300)
        try:
            await asyncio.sleep(max(5.0, interval))
        except asyncio.CancelledError:
            return
        mode = ((_settings or {}).get("s3") or {}).get("upload_mode", "immediate")
        if mode != "interval":
            continue  # 모드가 바뀌면 루프는 돌지만 flush 하지 않음
        try:
            await flush_once()
        except Exception:
            pass


def start_background():
    """interval 모드에서만 의미 있음. 호출은 startup event 에서."""
    global _bg_task
    if _bg_task is not None:
        return
    loop = asyncio.get_event_loop()
    _bg_task = loop.create_task(_loop())


def stop_background():
    global _bg_task
    if _bg_task is not None:
        _bg_task.cancel()
        _bg_task = None
