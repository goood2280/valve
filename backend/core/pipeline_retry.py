"""Persistent retry queue for failed pipeline raw units.

The normal pipeline keeps producing event/feature/wide outputs from the best
available data.  This store separately guarantees that a failed
vehicle/source/date unit remains eligible for retry even after it falls out of
the rolling ``raw_days`` window.

재시도는 무한하지 않다 — max_attempts(기본 5) 연속 실패하면 그 유닛은
``blocked`` 상태로 자동 재시도에서 빠진다. 그쯤이면 일시적 장애가 아니라
자격증명 만료·권한·테이블 변경처럼 사람이 고쳐야 하는 문제이고, 무한 재시도는
사내 API 를 계속 두드리기만 한다. 고친 뒤 resume() 으로 다시 큐에 넣는다.
"""
from __future__ import annotations

import json
import threading
import time
from datetime import date
from pathlib import Path

MAX_ATTEMPTS_DEFAULT = 5
BLOCKED_HINT = ("연속 실패로 자동 재시도를 멈췄습니다 — API key 만료·권한·테이블 변경 "
                "가능성을 확인하고, 해결 후 '재시도 재개' 를 누르세요")


class PipelineRetryStore:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._items: dict[str, dict] = {}
        self._load()

    @staticmethod
    def key(vehicle: str, source: str, start, end, split: str) -> str:
        return "|".join((str(vehicle), str(source), str(start), str(end), str(split)))

    def _load(self):
        """큐를 읽는다. 깨진 파일을 만나도 조용히 버리지 않는다 —
        여기가 비면 '놓친 유닛' 추적이 통째로 사라지는데 화면엔 정상으로 보인다.
        utf-8-sig 로 읽어 BOM 붙은 파일(운영자가 메모장으로 열어 저장)도 살린다."""
        self._items = {}
        if not self.path.exists():
            return
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8-sig"))
            self._items = dict(raw.get("items") or {})
        except Exception as e:
            try:    # 원본을 남겨 두고(덮어쓰기 전) 무엇이 사라졌는지 알 수 있게
                bad = self.path.with_suffix(self.path.suffix + ".bad")
                bad.write_bytes(self.path.read_bytes())
            except Exception:
                bad = None
            print(f"[valve] 재시도 큐를 읽지 못했습니다 ({self.path.name}): {e}"
                  + (f" — 원본 보관: {bad.name}" if bad else ""))

    def _save_locked(self):
        payload = {"version": 1, "updated_at": time.time(), "items": self._items}
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self.path)

    def record_failure(self, vehicle: str, source: str, start, end, split: str,
                       error: str, delays: list[int] | None = None,
                       now: float | None = None,
                       max_attempts: int = MAX_ATTEMPTS_DEFAULT) -> dict:
        now = float(now or time.time())
        delays = [max(1, int(v)) for v in (delays or [300, 900, 1800, 3600])]
        max_attempts = max(1, int(max_attempts or MAX_ATTEMPTS_DEFAULT))
        key = self.key(vehicle, source, start, end, split)
        with self._lock:
            prev = self._items.get(key) or {}
            attempts = int(prev.get("attempts") or 0) + 1
            delay = delays[min(attempts - 1, len(delays) - 1)]
            blocked = attempts >= max_attempts
            item = {
                "key": key,
                "vehicle": str(vehicle), "source": str(source),
                "start": str(start), "end": str(end), "split": str(split),
                "status": "blocked" if blocked else "retry_wait",
                "attempts": attempts,
                "total_attempts": int(prev.get("total_attempts") or 0) + 1,
                "first_failed_at": float(prev.get("first_failed_at") or now),
                "last_failed_at": now,
                # blocked 는 자동 재시도 대상이 아니다 — next_retry_at 을 비운다
                "next_retry_at": None if blocked else now + delay,
                "blocked_since": now if blocked else None,
                "max_attempts": max_attempts,
                "last_error": str(error)[:500],
            }
            self._items[key] = item
            self._save_locked()
            return dict(item)

    def resume(self, vehicle: str | None = None, key: str | None = None,
               now: float | None = None) -> int:
        """blocked 유닛을 다시 자동 재시도 대상으로 (원인을 고친 뒤 누르는 버튼).

        attempts 를 0 으로 되돌려 재시도 라운드를 새로 준다 — 그대로 두면 다음
        실패 한 번에 곧바로 다시 blocked 가 된다. 누적 횟수는 total_attempts 로 남는다."""
        now = float(now or time.time())
        n = 0
        with self._lock:
            for k, item in self._items.items():
                if key and k != key:
                    continue
                if vehicle and item.get("vehicle") != vehicle:
                    continue
                if item.get("status") != "blocked":
                    continue
                item.update({"status": "retry_wait", "attempts": 0,
                             "next_retry_at": now, "blocked_since": None,
                             "resumed_at": now})
                n += 1
            if n:
                self._save_locked()
        return n

    def mark_success(self, vehicle: str, source: str, start, end, split: str) -> dict | None:
        key = self.key(vehicle, source, start, end, split)
        with self._lock:
            item = self._items.pop(key, None)
            if item is not None:
                self._save_locked()
            return dict(item) if item else None

    def pending(self, vehicle: str | None = None) -> list[dict]:
        with self._lock:
            items = [dict(v) for v in self._items.values()
                     if not vehicle or v.get("vehicle") == vehicle]
        return sorted(items, key=lambda v: (float(v.get("next_retry_at") or 0), v.get("key") or ""))

    @staticmethod
    def _is_blocked(item: dict) -> bool:
        return item.get("status") == "blocked" or item.get("next_retry_at") is None

    def due_units(self, vehicle: str, now: float | None = None) -> list[tuple]:
        now = float(now or time.time())
        out = []
        for item in self.pending(vehicle):
            if self._is_blocked(item):
                continue                       # 자동 재시도 중단 — resume 필요
            if float(item.get("next_retry_at") or 0) > now:
                continue
            try:
                out.append((item["source"], date.fromisoformat(item["start"]),
                            date.fromisoformat(item["end"]), item["split"]))
            except (KeyError, TypeError, ValueError):
                continue
        return out

    def summary(self, vehicle: str | None = None, now: float | None = None) -> dict:
        now = float(now or time.time())
        items = self.pending(vehicle)
        blocked = [v for v in items if self._is_blocked(v)]
        waiting = [v for v in items if not self._is_blocked(v)]
        first = min((float(v.get("first_failed_at") or now) for v in items), default=None)
        # blocked 는 예정 시각이 없다 — 대기 중인 것만 보고 다음 재시도를 잡는다
        next_retry = min((float(v.get("next_retry_at") or now) for v in waiting), default=None)
        return {
            "pending": len(items),
            "waiting": len(waiting),
            "blocked": len(blocked),
            "due": sum(float(v.get("next_retry_at") or 0) <= now for v in waiting),
            "oldest_age_sec": round(now - first, 1) if first is not None else 0,
            "next_retry_at": next_retry,
            "max_attempts": max((int(v.get("attempts") or 0) for v in items), default=0),
            "hint": BLOCKED_HINT if blocked else "",
            "blocked_errors": sorted({str(v.get("last_error") or "")[:200] for v in blocked}),
            "items": items,
        }
