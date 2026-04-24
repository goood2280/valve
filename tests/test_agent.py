"""agent 라우터 — diagnose/suggest/apply-fix + 감사 로그 + cooldown."""
from __future__ import annotations


def test_actions_catalog(app_client):
    r = app_client.get("/api/agent/actions")
    assert r.status_code == 200
    names = {a["action"] for a in r.json()["actions"]}
    for a in ("retry_chunk", "retry_partition", "toggle_probe_skip",
              "invalidate_probe_cache", "reshard_source"):
        assert a in names


def test_diagnose_returns_list(app_client):
    r = app_client.get("/api/agent/diagnose")
    assert r.status_code == 200
    d = r.json()
    assert "anomalies" in d
    assert isinstance(d["anomalies"], list)


def test_apply_fix_rejects_unknown_action(app_client):
    r = app_client.post("/api/agent/apply-fix",
                        json={"action": "drop_everything", "args": {}})
    assert r.status_code == 400


def test_apply_fix_missing_arg(app_client):
    r = app_client.post("/api/agent/apply-fix",
                        json={"action": "retry_chunk", "args": {}})
    assert r.status_code == 400


def test_apply_fix_dry_run_for_toggle_probe_skip(app_client):
    r = app_client.post("/api/agent/apply-fix", json={
        "action": "toggle_probe_skip",
        "args": {"product": "PRODA", "source": "FAB", "value": True},
        "dry_run": True,
    })
    assert r.status_code == 200
    assert r.json()["ok"] is True
    assert "plan" in r.json()


def test_apply_fix_real_toggle_probe_skip_persists(app_client):
    # real apply
    r = app_client.post("/api/agent/apply-fix", json={
        "action": "toggle_probe_skip",
        "args": {"product": "PRODA", "source": "FAB", "value": True},
        "dry_run": False,
    })
    assert r.status_code == 200
    assert r.json()["ok"] is True

    # products.yaml 에 반영됐는지 확인
    prods = app_client.get("/api/schedule/products").json()
    fab = [s for s in prods["products"][0]["sources"] if s["name"] == "FAB"][0]
    assert fab.get("probe_skip") is True


def test_apply_fix_cooldown(app_client):
    # 1st real call OK
    r1 = app_client.post("/api/agent/apply-fix", json={
        "action": "invalidate_probe_cache", "args": {},
        "dry_run": False,
    })
    assert r1.status_code == 200
    # 2nd immediately → 429 cooldown
    r2 = app_client.post("/api/agent/apply-fix", json={
        "action": "invalidate_probe_cache", "args": {},
        "dry_run": False,
    })
    assert r2.status_code == 429


def test_apply_fix_high_safety_requires_confirm(app_client):
    # dry_run=false but no confirm_high_risk → 거부
    r = app_client.post("/api/agent/apply-fix", json={
        "action": "enqueue_product_seed",
        "args": {"product": "PRODA"},
        "dry_run": False,
    })
    assert r.status_code == 200  # 서버는 200 + ok=False 반환
    assert r.json()["ok"] is False


def test_suggest_fix_rule_based(app_client):
    # 아무 anomaly 하나 만들어 제공 (diagnose 가 빈 경우에 대비)
    anomaly = {
        "id": "fake-1", "kind": "probe_error",
        "product": "PRODA", "source": "FAB",
        "error": "TimeoutError",
    }
    r = app_client.post("/api/agent/suggest-fix",
                        json={"anomaly_id": "fake-1", "anomaly": anomaly})
    assert r.status_code == 200
    sugg = r.json()["suggestions"]
    assert any(s["action"] == "toggle_probe_skip" for s in sugg)


def test_audit_records_calls(app_client):
    app_client.get("/api/agent/diagnose")
    app_client.post("/api/agent/apply-fix", json={"action": "invalidate_probe_cache", "args": {},
                                                   "dry_run": True})
    r = app_client.get("/api/agent/audit?limit=10")
    assert r.status_code == 200
    assert r.json()["log_exists"] is True
    endpoints = {it.get("endpoint") for it in r.json()["items"]}
    assert "/api/agent/diagnose" in endpoints
