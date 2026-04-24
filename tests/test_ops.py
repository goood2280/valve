"""ops 라우터 — /api/metrics (JSON + Prometheus) + webhook 연결 테스트."""
from __future__ import annotations

import json
import threading
import socket
from http.server import BaseHTTPRequestHandler, HTTPServer


def test_metrics_json_basic_shape(app_client):
    r = app_client.get("/api/metrics")
    assert r.status_code == 200
    d = r.json()
    for k in ("chunk_status", "partition_status", "total_chunks",
              "total_plans", "total_rows_extracted",
              "duration_p50", "duration_p95", "duration_max",
              "running_chunks"):
        assert k in d, f"missing key {k}"


def test_metrics_prom_format(app_client):
    r = app_client.get("/api/metrics/prom")
    assert r.status_code == 200
    body = r.text
    assert "# HELP valve_total_chunks" in body
    assert "# TYPE valve_total_chunks counter" in body
    assert "valve_running_chunks" in body
    assert "valve_chunk_duration_p95_seconds" in body


def test_webhook_test_no_url_returns_error(app_client):
    r = app_client.post("/api/alerts/test", json={})
    assert r.status_code == 200
    assert r.json()["ok"] is False


def _start_capture_server():
    """간이 HTTP 캡처 — POST body 를 수집하는 로컬 서버."""
    captured = []
    host, port = "127.0.0.1", _free_port()

    class H(BaseHTTPRequestHandler):
        def do_POST(self):
            n = int(self.headers.get("Content-Length", "0"))
            data = self.rfile.read(n).decode("utf-8", errors="replace")
            try: captured.append(json.loads(data))
            except Exception: captured.append({"raw": data})
            self.send_response(200); self.end_headers()
            self.wfile.write(b'{"ok":true}')

        def log_message(self, *a, **k): pass

    server = HTTPServer((host, port), H)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return server, f"http://{host}:{port}/", captured


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0)); return s.getsockname()[1]


def test_webhook_test_hits_given_url(app_client):
    srv, url, captured = _start_capture_server()
    try:
        r = app_client.post("/api/alerts/test", json={"url": url})
        assert r.status_code == 200
        assert r.json()["ok"] is True
        assert any(e.get("kind") == "valve.test" for e in captured)
    finally:
        srv.shutdown()
