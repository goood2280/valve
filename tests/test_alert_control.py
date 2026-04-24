"""alerts 제어 — enabled / min_severity / rate_limit / dedupe / per-channel."""
from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_disabled_suppresses_all(app_client):
    app_client.post("/api/settings", json={"alerts": {"enabled": False}})
    from backend.routers import ops as _ops
    r = await _ops.dispatch_alert({"kind": "test", "severity": "error"})
    assert r["dispatch"]["suppressed"] == "alerts.enabled=false"


@pytest.mark.asyncio
async def test_min_severity_filter(app_client):
    app_client.post("/api/settings", json={"alerts": {"enabled": True, "min_severity": "error",
                                                       "max_per_hour": 0, "dedupe_window_sec": 0}})
    from backend.routers import ops as _ops
    # info < error → suppressed
    r = await _ops.dispatch_alert({"kind": "test_info", "severity": "info"})
    assert "suppressed" in r["dispatch"]
    # error == error → 발행 (채널 없지만 필터는 통과)
    r2 = await _ops.dispatch_alert({"kind": "test_err", "severity": "error"})
    assert "suppressed" not in r2.get("dispatch", {}) or r2["dispatch"].get("suppressed") is None


@pytest.mark.asyncio
async def test_rate_limit_cuts_off(app_client):
    app_client.post("/api/settings", json={"alerts": {"enabled": True, "min_severity": "info",
                                                       "max_per_hour": 2, "dedupe_window_sec": 0}})
    from backend.routers import ops as _ops
    _ops._recent_hour.clear(); _ops._dedupe_last.clear()
    r1 = await _ops.dispatch_alert({"kind": "rl1", "severity": "info"})
    r2 = await _ops.dispatch_alert({"kind": "rl2", "severity": "info"})
    r3 = await _ops.dispatch_alert({"kind": "rl3", "severity": "info"})
    assert "suppressed" not in r1.get("dispatch", {}) or r1["dispatch"].get("suppressed") is None
    assert "suppressed" not in r2.get("dispatch", {}) or r2["dispatch"].get("suppressed") is None
    assert r3["dispatch"]["suppressed"].startswith("rate_limit")


@pytest.mark.asyncio
async def test_dedupe_window(app_client):
    app_client.post("/api/settings", json={"alerts": {"enabled": True, "min_severity": "info",
                                                       "max_per_hour": 0, "dedupe_window_sec": 60}})
    from backend.routers import ops as _ops
    _ops._recent_hour.clear(); _ops._dedupe_last.clear()
    r1 = await _ops.dispatch_alert({"kind": "same", "chunk_id": "c1", "severity": "info"})
    r2 = await _ops.dispatch_alert({"kind": "same", "chunk_id": "c1", "severity": "info"})
    assert "suppressed" not in r1.get("dispatch", {}) or r1["dispatch"].get("suppressed") is None
    assert r2["dispatch"]["suppressed"].startswith("dedupe")


@pytest.mark.asyncio
async def test_per_channel_disable(app_client):
    """flow/s3 비활성 시 dispatch 결과에 None 이 기록됨 — 다른 채널이 돌든 말든."""
    app_client.post("/api/settings", json={"alerts": {
        "enabled": True, "min_severity": "info", "max_per_hour": 0, "dedupe_window_sec": 0,
        "webhook_url": "", "webhook_enabled": False,
        "flow_notify_url": "http://unreachable.invalid/", "flow_enabled": False,
        "s3_enabled": False,
    }})
    from backend.routers import ops as _ops
    _ops._recent_hour.clear(); _ops._dedupe_last.clear()
    r = await _ops.dispatch_alert({"kind": "chan_test", "severity": "warn"})
    # 모든 채널 비활성 → 모두 None
    assert r["dispatch"]["webhook"] is None
    assert r["dispatch"]["flow"] is None
    assert r["dispatch"]["s3"] is None
    # 하지만 메모리 버퍼에는 기록됨
    r2 = app_client.get("/api/alerts/recent?limit=5").json()
    assert any(x.get("kind") == "chan_test" for x in r2["items"])
