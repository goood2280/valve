"""ops 라우터 — 운영성(metrics + webhook alert).

Prometheus text format `/api/metrics` — 외부 의존성 0. 카운터·게이지 계산은
실행 이력(state.snapshot)에서 파생.

Webhook 은 chunk 가 실패하거나 probe 가 error 반환했을 때 settings.alerts.webhook_url
로 POST JSON. best-effort — 실패해도 메인 플로우 미차단.
"""
from __future__ import annotations

import asyncio
import json
import time
import urllib.error
import urllib.request
from collections import Counter

from fastapi import APIRouter
from fastapi.responses import PlainTextResponse

router = APIRouter(tags=["ops"])

_state = None
_settings = None
_s3 = None                               # S3 알람 업로드용
_last_webhook_by_chunk: dict[str, float] = {}
_webhook_cooldown_sec = 60
_recent_alerts: list[dict] = []          # UI 가 최근 알람 조회 가능하도록 in-memory
_MAX_RECENT = 200


def deps(state, settings, s3=None):
    global _state, _settings, _s3
    _state = state
    _settings = settings
    _s3 = s3


def _metrics_snapshot() -> dict:
    """state snapshot → metrics counters."""
    snap = _state.snapshot() if _state else {"plans": {}, "chunks": {}, "partitions": {}}
    chunks = list((snap.get("chunks") or {}).values())
    partitions = list((snap.get("partitions") or {}).values())

    status_count = Counter((c.get("status") or "unknown") for c in chunks)
    part_status = Counter((p.get("status") or "unknown") for p in partitions)

    durations = [c.get("duration_sec") for c in chunks if isinstance(c.get("duration_sec"), (int, float))]
    total_rows = sum((c.get("actual_rows") or 0) for c in chunks if c.get("status") == "success")
    running = sum(1 for c in chunks if c.get("status") in ("in_progress", "running"))
    plans = len(snap.get("plans") or {})

    return {
        "chunk_status": status_count,
        "partition_status": part_status,
        "total_chunks": len(chunks),
        "total_plans": plans,
        "total_rows_extracted": total_rows,
        "duration_p50": _percentile(durations, 50),
        "duration_p95": _percentile(durations, 95),
        "duration_max": max(durations) if durations else 0.0,
        "running_chunks": running,
    }


def _percentile(vals: list[float], p: int) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    k = max(0, min(len(s) - 1, int(round(p / 100.0 * (len(s) - 1)))))
    return float(s[k])


@router.get("/api/metrics")
def metrics_json():
    """JSON 버전 — 대시보드·디버깅용."""
    return _metrics_snapshot()


@router.get("/api/metrics/prom", response_class=PlainTextResponse)
def metrics_prom():
    """Prometheus text format — scrape 대상. 최소 5종 지표 + per-status counter."""
    m = _metrics_snapshot()
    lines = []

    def add(name: str, help_text: str, value, kind: str = "counter", labels: dict | None = None):
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} {kind}")
        lbl = ("{" + ",".join(f'{k}="{v}"' for k, v in (labels or {}).items()) + "}") if labels else ""
        lines.append(f"{name}{lbl} {value}")

    add("valve_total_chunks", "누적 기록된 chunk 수", m["total_chunks"])
    add("valve_total_plans", "누적 기록된 plan 수", m["total_plans"])
    add("valve_running_chunks", "현재 실행중 chunk 수", m["running_chunks"], kind="gauge")
    add("valve_total_rows_extracted", "success chunk 의 actual_rows 합계", m["total_rows_extracted"])
    add("valve_chunk_duration_p50_seconds", "chunk 실행 시간 p50(초)", m["duration_p50"], kind="gauge")
    add("valve_chunk_duration_p95_seconds", "chunk 실행 시간 p95(초)", m["duration_p95"], kind="gauge")
    add("valve_chunk_duration_max_seconds", "chunk 실행 시간 max(초)", m["duration_max"], kind="gauge")

    for st, n in (m["chunk_status"] or {}).items():
        add("valve_chunk_status_count", "status 별 chunk 수",
            n, kind="counter", labels={"status": st})
    for st, n in (m["partition_status"] or {}).items():
        add("valve_partition_status_count", "status 별 partition 수",
            n, kind="counter", labels={"status": st})

    return "\n".join(lines) + "\n"


@router.post("/api/alerts/test")
async def alerts_test(req: dict):
    """webhook 연결 테스트. req 에 {url?} 지정 시 그 url 로 핑, 없으면 settings 값 사용."""
    url = (req or {}).get("url") or _webhook_url()
    if not url:
        return {"ok": False, "error": "no webhook url configured"}
    ok, msg = await _post_webhook(url, {"kind": "valve.test", "ts": time.time(),
                                        "message": "Valve webhook 연결 테스트"})
    return {"ok": ok, "url": url, "message": msg}


def _webhook_url() -> str:
    if not _settings:
        return ""
    return ((_settings.get("alerts") or {}).get("webhook_url") or "").strip()


async def _post_webhook(url: str, payload: dict) -> tuple[bool, str]:
    def _send():
        data = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        req = urllib.request.Request(
            url, data=data, method="POST",
            headers={"Content-Type": "application/json", "User-Agent": "Valve-webhook/0.1"},
        )
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, r.read().decode("utf-8", errors="replace")[:200]
    try:
        status, body = await asyncio.to_thread(_send)
        return 200 <= status < 300, f"{status} {body}"
    except urllib.error.HTTPError as e:
        return False, f"{e.code} {e.reason}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


async def emit_failure_webhook(evt: dict):
    """외부에서 호출 — chunk 실패/probe 실패 발생 시. cooldown 60s 걸어 폭주 방지."""
    url = _webhook_url()
    if not url:
        return
    key = evt.get("chunk_id") or evt.get("plan_id") or ""
    now = time.time()
    last = _last_webhook_by_chunk.get(key, 0.0)
    if now - last < _webhook_cooldown_sec:
        return
    _last_webhook_by_chunk[key] = now
    await _post_webhook(url, {"kind": "valve.failure", "ts": now, **evt})


# ─────────────────────────────────────────────────
# 통합 알람 — 3채널(S3 · flow · generic webhook) 병렬 dispatch.
# config fallback · chunk 실패 · probe 실패 등 모든 "이상" 이벤트를 여기로.
# ─────────────────────────────────────────────────
def _alert_s3_prefix() -> str:
    if not _settings:
        return ""
    return ((_settings.get("alerts") or {}).get("s3_prefix") or "valve-alerts").strip("/")


def _flow_notify_url() -> str:
    if not _settings:
        return ""
    return ((_settings.get("alerts") or {}).get("flow_notify_url") or "").strip()


def record_alert(evt: dict):
    """메모리 버퍼에 최근 알람 기록. async 호출 없이 sync 로 바로 저장 — UI 조회용.
    dispatch_alert 가 내부적으로 호출. 외부에서도 테스트/fallback 용도로 호출 가능."""
    evt = dict(evt)
    evt.setdefault("ts", time.time())
    _recent_alerts.append(evt)
    if len(_recent_alerts) > _MAX_RECENT:
        del _recent_alerts[: len(_recent_alerts) - _MAX_RECENT]


async def dispatch_alert(evt: dict):
    """3-채널 fan-out. 각 채널 실패가 다른 채널을 막지 않음.
    각 채널의 성공/실패는 alert 자체의 meta 에 반영(재귀 알람 없음)."""
    import asyncio
    evt = dict(evt)
    evt.setdefault("ts", time.time())
    evt.setdefault("source", "valve")
    record_alert(evt)

    # 1) S3 알람 업로드 (옵션)
    results = {"s3": None, "flow": None, "webhook": None}
    if _s3 is not None:
        prefix = _alert_s3_prefix()
        if prefix:
            ts_key = time.strftime("%Y%m%dT%H%M%S", time.gmtime(evt["ts"]))
            key = f"{prefix}/{ts_key}-{evt.get('kind','event')}.json"
            try:
                ok = await asyncio.to_thread(
                    _s3.put_text, key, json.dumps(evt, ensure_ascii=False, default=str))
                results["s3"] = {"ok": bool(ok), "key": key}
            except Exception as e:
                results["s3"] = {"ok": False, "error": f"{type(e).__name__}: {e}"}

    # 2) flow 앱 알람 엔드포인트 (옵션)
    flow_url = _flow_notify_url()
    if flow_url:
        ok, msg = await _post_webhook(flow_url, evt)
        results["flow"] = {"ok": ok, "message": msg}

    # 3) 일반 webhook (옵션) — 기존 경로 재사용
    web_url = _webhook_url()
    if web_url and web_url != flow_url:
        ok, msg = await _post_webhook(web_url, evt)
        results["webhook"] = {"ok": ok, "message": msg}

    evt["dispatch"] = results
    return evt


def emit_alert_sync(evt: dict):
    """sync 컨텍스트(기동 시퀀스 등)에서 알람 발행.
    이미 돌고 있는 이벤트루프가 있으면 create_task, 없으면 나중 발행 큐에 저장."""
    import asyncio
    record_alert(evt)
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop is not None and loop.is_running():
        loop.create_task(dispatch_alert(evt))
        return
    # 기동 단계 — 나중에 dispatch 되도록 보류 큐에
    _pending_alerts.append(evt)


_pending_alerts: list[dict] = []


async def flush_pending_alerts():
    """앱 startup 끝난 뒤 FastAPI startup event 에서 호출 — 기동 중 발생한 알람 발행."""
    import asyncio
    while _pending_alerts:
        evt = _pending_alerts.pop(0)
        try:
            await dispatch_alert(evt)
        except Exception:
            pass


@router.get("/api/alerts/recent")
def alerts_recent(limit: int = 50):
    """UI·에이전트가 최근 알람 조회."""
    limit = max(1, min(int(limit or 50), _MAX_RECENT))
    return {"items": list(reversed(_recent_alerts))[:limit], "count": len(_recent_alerts)}
