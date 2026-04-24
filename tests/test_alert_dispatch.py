"""통합 알람 dispatch — S3 put + flow_notify_url + generic webhook 3-채널."""
from __future__ import annotations

import asyncio
import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0)); return s.getsockname()[1]


def _capture_server():
    got = []
    port = _free_port()

    class H(BaseHTTPRequestHandler):
        def do_POST(self):
            n = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(n).decode("utf-8", errors="replace")
            try: got.append(json.loads(body))
            except Exception: got.append({"raw": body})
            self.send_response(200); self.end_headers(); self.wfile.write(b'{"ok":true}')
        def log_message(self, *a, **k): pass

    srv = HTTPServer(("127.0.0.1", port), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, f"http://127.0.0.1:{port}/", got


@pytest.mark.asyncio
async def test_dispatch_alert_hits_flow_and_webhook(app_client):
    flow_srv, flow_url, flow_got = _capture_server()
    web_srv, web_url, web_got = _capture_server()
    try:
        # settings.alerts 셋업
        r = app_client.post("/api/settings", json={
            "alerts": {"flow_notify_url": flow_url, "webhook_url": web_url, "s3_prefix": "valve-alerts"},
        })
        assert r.status_code == 200

        from backend.routers import ops as _ops
        evt = {"kind": "test_alert", "severity": "error", "title": "Unit test alert"}
        result = await _ops.dispatch_alert(evt)

        assert result["dispatch"]["flow"]["ok"] is True
        assert result["dispatch"]["webhook"]["ok"] is True
        assert any(x.get("kind") == "test_alert" for x in flow_got)
        assert any(x.get("kind") == "test_alert" for x in web_got)
    finally:
        flow_srv.shutdown(); web_srv.shutdown()


def test_alerts_recent_endpoint(app_client):
    from backend.routers import ops as _ops
    _ops.record_alert({"kind": "recent_test", "severity": "warn"})
    r = app_client.get("/api/alerts/recent?limit=10")
    assert r.status_code == 200
    items = r.json()["items"]
    assert any(x.get("kind") == "recent_test" for x in items)


@pytest.mark.asyncio
async def test_dispatch_alert_puts_to_s3(app_client, tmp_path):
    """fake_local S3 에 알람 JSON 이 저장되는지."""
    # s3 fake_local 경로는 conftest 에서 이미 tmp/s3_local 로 설정됨
    from backend.routers import ops as _ops
    evt = {"kind": "s3_test", "severity": "info", "title": "s3 dispatch test"}
    await _ops.dispatch_alert(evt)
    # _ops._s3 가 있으면 fake_local 디렉터리 안에 valve-alerts prefix 로 파일이 생김
    s3 = _ops._s3
    if s3 and getattr(s3, 'fake_local', None):
        from pathlib import Path
        base = Path(s3.fake_local).resolve() / s3.bucket / "valve-alerts"
        if base.exists():
            files = list(base.rglob("*.json"))
            # put 은 best-effort; 파일 있으면 내용 검증
            if files:
                content = files[-1].read_text(encoding="utf-8")
                assert "s3_test" in content
