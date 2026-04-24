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
_last_webhook_by_chunk: dict[str, float] = {}
_webhook_cooldown_sec = 60


def deps(state, settings):
    global _state, _settings
    _state = state
    _settings = settings


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
