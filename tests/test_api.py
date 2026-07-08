"""API layer — FastAPI endpoints via TestClient.
settings / schedule / jobs.history / source-types / columns.
"""
from __future__ import annotations


def test_health_ok(app_client):
    r = app_client.get("/api/health")
    assert r.status_code == 200
    d = r.json()
    assert d["ok"] is True
    assert d["lake_mode"] in ("mock", "real")


def test_version(app_client):
    r = app_client.get("/api/version")
    assert r.status_code == 200
    assert "version" in r.json()


def test_settings_crud_masks_secret(app_client):
    r = app_client.get("/api/settings")
    assert r.status_code == 200
    d = r.json()
    # s3.secret_key 가 평문으로 노출되지 않아야 함 (빈 문자열이면 통과)
    assert d["s3"]["secret_key"] in ("", "****")


def test_products_round_trip(app_client, sample_products):
    r = app_client.get("/api/schedule/products")
    assert r.status_code == 200
    assert any(p["product"] == "PRODA" for p in r.json()["products"])

    modified = {
        "products": sample_products["products"] + [
            {"product": "PRODC", "enabled": True, "priority": 30,
             "sources": [{"name": "FAB", "table": "RAW_FAB_DATA",
                          "shard_hierarchy": [], "target_chunk_rows": 500_000}],
             "params_template": {"product_code": {"op": "eq", "value": "PRODC"}},
             "custom_col": ["lot_id", "wafer_id"]}
        ]
    }
    r2 = app_client.post("/api/schedule/products", json=modified)
    assert r2.status_code == 200
    assert r2.json()["count"] == 3

    r3 = app_client.get("/api/schedule/products")
    assert any(p["product"] == "PRODC" for p in r3.json()["products"])


def test_schedule_honors_backfill_override(app_client):
    r = app_client.get("/api/schedule")
    assert r.status_code == 200
    d = r.json()
    # PRODB 는 backfill_days_override = 5 → max_backfill_days 가 전역(2) 보다 큼
    assert d["max_backfill_days"] >= 5
    prodb_dates = {it["date"] for it in d["items"] if it["product"] == "PRODB"}
    assert len(prodb_dates) >= 6  # today + 5 past


def test_source_types_list_includes_all_canonical(app_client):
    r = app_client.get("/api/schedule/source-types")
    assert r.status_code == 200
    names = {(s["name"] or "").upper() for s in r.json()["source_types"]}
    for canon in ("FAB", "INLINE", "VM"):
        assert canon in names, f"missing canonical source {canon}"
    # 추출 대상은 3종만 — 구 소스는 registry 에서 제거됨
    for legacy in ("ET", "QTIME", "EDS"):
        assert legacy not in names, f"legacy source {legacy} should be removed"


def test_source_types_add_and_remove(app_client):
    r = app_client.get("/api/schedule/source-types")
    current = r.json()["source_types"]
    # Custom 추가
    current.append({
        "name": "CUSTOMDB1", "table_template": "RAW_CUSTOM_DATA",
        "columns": ["lot_id", "wafer_id", "time"], "default_shard": [],
        "accent": "#e11d48", "hint": "custom hint",
    })
    r2 = app_client.post("/api/schedule/source-types", json={"source_types": current})
    assert r2.status_code == 200

    r3 = app_client.get("/api/schedule/columns?source=CUSTOMDB1")
    cols = r3.json()["columns"]
    assert "lot_id" in cols and "wafer_id" in cols

    # duplicate 방지
    bad = current + [{"name": "FAB", "table_template": "RAW_FAB_DATA"}]
    r4 = app_client.post("/api/schedule/source-types", json={"source_types": bad})
    assert r4.status_code == 400


def test_columns_merges_saved_custom_col(app_client):
    """저장된 source-level custom_col 은 /columns 응답에 합쳐져서 UI 누락 방지."""
    # PRODA/FAB 에 'my_custom_col' 추가
    prods = app_client.get("/api/schedule/products").json()
    prods["products"][0]["sources"][0]["custom_col"] = ["lot_id", "my_extra_col"]
    r = app_client.post("/api/schedule/products", json=prods)
    assert r.status_code == 200

    r2 = app_client.get("/api/schedule/columns?product=PRODA&source=FAB")
    assert "my_extra_col" in r2.json()["columns"]


def test_history_returns_list(app_client):
    r = app_client.get("/api/jobs/history?limit=10")
    assert r.status_code == 200
    d = r.json()
    assert isinstance(d.get("items"), list)


def test_history_failed_only_filter(app_client):
    r = app_client.get("/api/jobs/history?failed_only=true&kind=chunk")
    assert r.status_code == 200
    for it in r.json()["items"]:
        assert it["status"] in ("failed", "timeout_reshard",
                                 "completeness_failed", "upload_failed")


def test_history_product_filter(app_client):
    r = app_client.get("/api/jobs/history?kind=chunk&product=PRODA")
    assert r.status_code == 200
    for it in r.json()["items"]:
        assert it["product"] == "PRODA"


def test_enqueue_product_requires_product(app_client):
    r = app_client.post("/api/jobs/enqueue-product", json={})
    assert r.status_code == 400


def test_enqueue_product_unknown_404(app_client):
    r = app_client.post("/api/jobs/enqueue-product", json={"product": "NOPE"})
    assert r.status_code == 404
