"""API layer — FastAPI endpoints via TestClient.
settings / schedule / jobs.history / source-types / columns.
"""
from __future__ import annotations


def test_health_ok(app_client):
    r = app_client.get("/api/health")
    assert r.status_code == 200
    d = r.json()
    assert d["ok"] is True
    assert d["lake_mode"] == "real"
    assert d["db_root"]
    assert d["staging"].replace("\\", "/").endswith("/db/0.STAGING")


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


def test_settings_rejects_mock_adapter(app_client):
    r = app_client.post("/api/settings", json={
        "lake_api": {"mode": "mock", "module": "valve.mock:query"},
    })
    assert r.status_code == 400


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


def _source_types(app_client) -> list[dict]:
    return app_client.get("/api/schedule/source-types").json()["source_types"]


def _product_source(app_client, product: str, source: str) -> dict:
    prods = app_client.get("/api/schedule/products").json()["products"]
    p = next(x for x in prods if x["product"] == product)
    return next(s for s in p["sources"] if (s.get("name") or "").upper() == source)


def test_source_type_table_template_propagates_to_products(app_client):
    """소스 타입의 table_template 을 바꿔 저장하면 그 소스를 쓰는 제품 table 도 함께 바뀐다."""
    types = _source_types(app_client)
    for st in types:
        if st["name"] == "FAB":
            st["table_template"] = "RAW_{name}_V2"
    r = app_client.post("/api/schedule/source-types", json={"source_types": types})
    assert r.status_code == 200
    applied = r.json()["products"]
    tables = {(c["product"], c["source"]): c["to"] for c in applied["changes"] if c["field"] == "table"}
    assert tables == {("PRODA", "FAB"): "RAW_FAB_V2", ("PRODB", "FAB"): "RAW_FAB_V2"}
    assert not applied["conflicts"]
    # 파일·런타임 양쪽 반영
    assert _product_source(app_client, "PRODA", "FAB")["table"] == "RAW_FAB_V2"
    assert _product_source(app_client, "PRODB", "FAB")["table"] == "RAW_FAB_V2"
    # 다른 소스는 그대로
    assert _product_source(app_client, "PRODA", "INLINE")["table"] == "RAW_INLINE_DATA"


def test_source_type_rename_renames_product_sources(app_client):
    """이름 변경(prev_name) 은 제품 소스명·table 까지 따라간다. prev_name 은 yaml 에 안 남는다."""
    types = _source_types(app_client)
    for st in types:
        if st["name"] == "INLINE":
            st["prev_name"] = "INLINE"
            st["name"] = "INLINE2"
    r = app_client.post("/api/schedule/source-types", json={"source_types": types})
    assert r.status_code == 200
    src = _product_source(app_client, "PRODA", "INLINE2")
    assert src["table"] == "RAW_INLINE2_DATA"
    assert not any(s["name"] == "INLINE" for s in
                   app_client.get("/api/schedule/products").json()["products"][0]["sources"])
    assert all("prev_name" not in st for st in _source_types(app_client))
    # 남아있는 다른 제품/소스는 orphan 으로 보고되지 않음 (PRODB 는 FAB 만 씀)
    assert r.json()["products"]["orphans"] == []


def test_source_type_save_keeps_product_specific_table_until_forced(app_client):
    """제품에서 직접 지정한 table 은 유지하고 conflict 로만 보고, force_table 이면 덮어쓴다."""
    prods = app_client.get("/api/schedule/products").json()
    prods["products"][0]["sources"][0]["table"] = "MY_SPECIAL_FAB"
    assert app_client.post("/api/schedule/products", json=prods).status_code == 200

    types = _source_types(app_client)
    for st in types:
        if st["name"] == "FAB":
            st["table_template"] = "RAW_{name}_V3"
    r = app_client.post("/api/schedule/source-types", json={"source_types": types})
    applied = r.json()["products"]
    assert applied["conflicts"] == [
        {"product": "PRODA", "source": "FAB", "current": "MY_SPECIAL_FAB", "template": "RAW_FAB_V3"}]
    assert _product_source(app_client, "PRODA", "FAB")["table"] == "MY_SPECIAL_FAB"
    assert _product_source(app_client, "PRODB", "FAB")["table"] == "RAW_FAB_V3"

    r2 = app_client.post("/api/schedule/source-types",
                         json={"source_types": types, "force_table": True})
    assert r2.json()["products"]["conflicts"] == []
    assert _product_source(app_client, "PRODA", "FAB")["table"] == "RAW_FAB_V3"


def test_source_type_save_can_opt_out_of_propagation(app_client):
    types = _source_types(app_client)
    for st in types:
        if st["name"] == "FAB":
            st["table_template"] = "RAW_{name}_V9"
    r = app_client.post("/api/schedule/source-types",
                        json={"source_types": types, "apply_to_products": False})
    assert r.json()["products"]["changes"] == []
    assert _product_source(app_client, "PRODA", "FAB")["table"] == "RAW_FAB_DATA"


def test_source_type_delete_reports_orphan_products(app_client):
    """레지스트리에서 소스를 지워도 제품 설정은 건드리지 않고 orphan 으로만 보고."""
    types = [st for st in _source_types(app_client) if st["name"] != "INLINE"]
    r = app_client.post("/api/schedule/source-types", json={"source_types": types})
    assert r.json()["products"]["orphans"] == [{"product": "PRODA", "source": "INLINE"}]
    assert _product_source(app_client, "PRODA", "INLINE")["table"] == "RAW_INLINE_DATA"


def test_source_type_default_shard_follows_only_when_untouched(app_client):
    """shard 는 제품이 '이전 기본값 그대로'일 때만 따라간다 — 손댄 값은 보존."""
    prods = app_client.get("/api/schedule/products").json()
    prods["products"][0]["sources"][0]["shard_hierarchy"] = []          # FAB: 기본값과 동일
    prods["products"][0]["sources"][1]["shard_hierarchy"] = ["lot_id"]  # INLINE: 직접 지정
    assert app_client.post("/api/schedule/products", json=prods).status_code == 200

    types = _source_types(app_client)
    for st in types:
        if st["name"] == "FAB":
            st["default_shard"] = ["root_lot_id"]
        if st["name"] == "INLINE":
            st["default_shard"] = ["root_lot_id", "lot_id"]
    app_client.post("/api/schedule/source-types", json={"source_types": types})
    assert _product_source(app_client, "PRODA", "FAB")["shard_hierarchy"] == ["root_lot_id"]
    assert _product_source(app_client, "PRODA", "INLINE")["shard_hierarchy"] == ["lot_id"]


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
