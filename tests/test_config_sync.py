"""ConfigSync — S3 → local fallback 3단계 (s3 → local → last_good → bundled).
각 단계 전환이 alert 를 내는지도 확인.
"""
from __future__ import annotations

import json
from pathlib import Path

import yaml

from backend.core.config_sync import ConfigSync


class FakeS3:
    """S3Uploader 의 get_text 흉내."""
    def __init__(self, store: dict | None = None, fail: bool = False):
        self.store = store or {}
        self.fail = fail
        self.put_calls = []

    def get_text(self, key: str):
        if self.fail:
            raise RuntimeError("s3 down")
        return self.store.get(key)

    def put_text(self, key: str, text: str):
        self.put_calls.append((key, text))
        return True


def test_sync_pulls_from_s3_and_backs_up(tmp_path):
    s3 = FakeS3({"valve-config/settings.json": json.dumps({"x": 1})})
    alerts = []
    cs = ConfigSync(s3, tmp_path, alert_cb=alerts.append)
    r = cs.sync("settings.json", parser=json.loads)
    assert r["source"] == "s3"
    assert r["changed"] is True
    assert (tmp_path / "settings.json").read_text().startswith("{")
    assert (tmp_path / "settings.json.last_good").exists()
    assert alerts == []


def test_sync_falls_back_to_local_when_s3_down(tmp_path):
    (tmp_path / "settings.json").write_text(json.dumps({"local": True}))
    s3 = FakeS3(fail=True)
    alerts = []
    cs = ConfigSync(s3, tmp_path, alert_cb=alerts.append)
    r = cs.sync("settings.json", parser=json.loads)
    assert r["source"] == "local"
    # S3 unreachable 알람 발행
    assert any(a["kind"] == "config_s3_unreachable" for a in alerts)


def test_sync_falls_back_to_last_good_when_local_corrupt(tmp_path):
    (tmp_path / "settings.json").write_text("{ broken json")
    (tmp_path / "settings.json.last_good").write_text(json.dumps({"ok": True}))
    s3 = FakeS3(fail=True)  # s3 도 안 됨
    alerts = []
    cs = ConfigSync(s3, tmp_path, alert_cb=alerts.append)
    r = cs.sync("settings.json", parser=json.loads)
    assert r["source"] == "last_good"
    assert r["changed"] is True
    assert any(a["kind"] == "config_local_corrupt" for a in alerts)
    assert any(a["kind"] == "config_fallback_last_good" for a in alerts)
    # local 이 last_good 로 복구됐는지
    assert json.loads((tmp_path / "settings.json").read_text()) == {"ok": True}


def test_sync_alerts_when_all_sources_missing(tmp_path):
    s3 = FakeS3({})  # 키 없음
    alerts = []
    cs = ConfigSync(s3, tmp_path, alert_cb=alerts.append)
    r = cs.sync("settings.json", parser=json.loads)
    assert r["source"] == "bundled"
    assert any(a["kind"] == "config_missing" and a["severity"] == "error" for a in alerts)


def test_s3_content_invalid_falls_back_to_local(tmp_path):
    # S3 는 손상된 JSON, 로컬은 정상
    (tmp_path / "settings.json").write_text(json.dumps({"good": True}))
    s3 = FakeS3({"valve-config/settings.json": "{ broken"})
    alerts = []
    cs = ConfigSync(s3, tmp_path, alert_cb=alerts.append)
    r = cs.sync("settings.json", parser=json.loads)
    assert r["source"] == "local"
    assert any(a["kind"] == "config_s3_invalid" for a in alerts)


def test_yaml_parser_works(tmp_path):
    s3 = FakeS3({"valve-config/products.yaml": yaml.safe_dump({"products": [{"product": "X"}]})})
    cs = ConfigSync(s3, tmp_path)
    r = cs.sync("products.yaml", parser=yaml.safe_load, kind="yaml")
    assert r["source"] == "s3"
    loaded = yaml.safe_load((tmp_path / "products.yaml").read_text())
    assert loaded["products"][0]["product"] == "X"
