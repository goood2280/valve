"""params_template 스키마 마이그레이션 — 구 포맷 {slot: {column,op,value}} → 신 포맷 {column: {op,value}}."""
from __future__ import annotations

import importlib
import json
import os
import sys
from pathlib import Path


def test_migration_converts_old_params_template(tmp_path):
    import yaml
    # 구 포맷 products.yaml
    old_products = {
        "products": [
            {
                "product": "LEGACY",
                "enabled": True,
                "priority": 10,
                "sources": [{"name": "FAB", "table": "RAW_FAB_DATA",
                             "shard_hierarchy": [], "target_chunk_rows": 500_000}],
                "params_template": {
                    "cata": {"column": "product_code", "op": "eq", "value": "LEGACY"},
                    "catb": {"column": "process_id", "op": "in", "value": ["P1", "P2"]},
                },
                "custom_col": ["lot_id"],
            }
        ]
    }
    (tmp_path / "config").mkdir()
    (tmp_path / "logs").mkdir()
    (tmp_path / "staging").mkdir()
    (tmp_path / "s3_local").mkdir()
    (tmp_path / "config" / "products.yaml").write_text(
        yaml.safe_dump(old_products, allow_unicode=True, sort_keys=False), encoding="utf-8")
    (tmp_path / "config" / "settings.json").write_text(json.dumps({
        "lake_api": {"mode": "mock", "module": "", "user": "t", "api_key": "",
                     "timeout_sec": 10, "min_interval_sec": 0, "max_concurrent": 1,
                     "retry": {"attempts": 1, "backoff_sec": [0]},
                     "retryable_errors": []},
        "s3": {"endpoint_url": "", "bucket": "t", "prefix": "", "access_key": "",
               "secret_key": "", "fake_local_path": "s3_local"},
        "schedule": {"backfill_days": 2, "interval_hours": 0,
                     "force_overwrite": False, "tolerance_pct": 0.5},
        "probe": {"strategy": "sample_window", "window_hours": 1, "cache_days": 7,
                  "adaptive_correction": True, "fallback_on_timeout": True},
    }), encoding="utf-8")

    os.environ["VALVE_ROOT"] = str(tmp_path)
    if "app" in sys.modules:
        del sys.modules["app"]
    app_module = importlib.import_module("app")

    migrated = yaml.safe_load((tmp_path / "config" / "products.yaml").read_text(encoding="utf-8"))
    tpl = migrated["products"][0]["params_template"]

    # 신 포맷: column 명이 키, column 필드는 사라짐
    assert "product_code" in tpl
    assert "process_id" in tpl
    assert tpl["product_code"] == {"op": "eq", "value": "LEGACY"}
    assert tpl["process_id"] == {"op": "in", "value": ["P1", "P2"]}
    assert "cata" not in tpl
    assert "catb" not in tpl


def test_new_format_untouched(tmp_path):
    """이미 신 포맷이면 아무 것도 바꾸지 않음."""
    from app import _migrate_params_template
    new_fmt = {
        "products": [{
            "product": "X",
            "params_template": {"product_code": {"op": "eq", "value": "X"}},
        }]
    }
    changed = _migrate_params_template(new_fmt)
    assert changed is False
    assert new_fmt["products"][0]["params_template"] == {"product_code": {"op": "eq", "value": "X"}}
