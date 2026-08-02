from __future__ import annotations

import sys
import types

import polars as pl
import pytest

from backend.core.executor import ChunkExecutor
from backend.core.lake_api import LakeAPI
from backend.core.planner import Chunk, ChunkPlan
from backend.core.s3_up import S3Uploader
from backend.core.state import StateStore
from backend.routers import browser, query


class RecordingLake:
    def __init__(self):
        self.calls = []

    async def query(self, params, custom_col):
        self.calls.append((dict(params), list(custom_col)))
        return pl.DataFrame({
            "ROOT_LOT_ID": ["R1"], "WAFER_ID": [1], "FAB_CD": [12.3],
        })


@pytest.mark.asyncio
async def test_partition_uses_table_name_persists_db_and_skips_disabled_s3(tmp_path):
    lake = RecordingLake()
    settings = {
        "lake_api": {"max_concurrent": 1},
        "schedule": {"tolerance_pct": 0.5},
        "s3": {"enabled": False, "upload_mode": "immediate"},
    }
    s3 = S3Uploader(settings)
    state = StateStore(tmp_path / "logs" / "jobs.jsonl")
    executor = ChunkExecutor(
        lake, None, s3, state, settings, tmp_path / "db" / "0.STAGING",
        db_root=tmp_path / "db", product_vehicles={"PRODA": ["VH_PRODA"]},
    )
    executor.product_filters = {"PRODA": {"process_id": "P100", "line_id": ["L1"]}}
    chunk = Chunk("PRODA-FAB-2026-07-31-00", "PRODA", "FAB", "2026-07-31")
    plan = ChunkPlan("PRODA-FAB-2026-07-31", "PRODA", "FAB", "2026-07-31",
                     [chunk], {"strategy": "none"})

    result = await executor.run_plan(
        plan,
        {"params_template": {"product_code": {"op": "eq", "value": "PRODA"}},
         "custom_col": ["ROOT_LOT_ID", "WAFER_ID", "FAB_CD"]},
        {"name": "FAB", "table": "RAW_FAB_DATA"},
    )

    params = lake.calls[0][0]
    assert params["table_name"] == "RAW_FAB_DATA"
    assert params["datefrom"] == "2026-07-31T00:00:00"
    assert params["dateto"] == "2026-08-01T00:00:00"
    assert params["process_id"] == "P100"
    assert params["line_id"] == ["L1"]
    assert lake.calls[0][1] == []
    assert "product_code" not in params
    assert "table" not in params
    assert result["ok"] is True
    assert result["upload_status"] == "skipped_disabled"
    assert result["s3_key"] is None
    db_file = (tmp_path / "db" / "1.RAWDATA_DB" / "FAB" / "VH_PRODA" /
               "date=2026-07-31" / "data.parquet")
    assert db_file.exists()
    assert pl.read_parquet(db_file).height == 1
    partition = state.snapshot()["partitions"]["PRODA/FAB/2026-07-31"]
    assert partition["status"] == "success"
    assert partition["s3_status"] == "skipped_disabled"


def test_real_adapter_normalizes_legacy_table_key(monkeypatch):
    seen = {}
    seen_kwargs = {}

    def get_data(params, **kwargs):
        seen.update(params)
        seen_kwargs.update(kwargs)
        return []

    monkeypatch.setitem(sys.modules, "bigdataquery", types.SimpleNamespace(getData=get_data))
    from backend.core.real_lake_adapter import query as real_query

    real_query({"table": "RAW_VM_DATA"}, [], "tester")
    assert seen == {"table_name": "RAW_VM_DATA"}
    assert seen_kwargs == {"user_name": "tester"}


@pytest.mark.asyncio
async def test_lake_api_normalizes_table_for_any_configured_adapter(monkeypatch):
    seen = {}

    def query_fn(params, custom_col, user):
        seen.update(params)
        return []

    monkeypatch.setitem(sys.modules, "test_valve_adapter",
                        types.SimpleNamespace(query=query_fn))
    api = LakeAPI({"lake_api": {
        "module": "test_valve_adapter:query", "user": "tester",
        "timeout_sec": 5, "min_interval_sec": 0,
        "retry": {"attempts": 1, "backoff_sec": [0]},
        "retryable_errors": [],
    }})

    await api.query({"table": "RAW_FAB_DATA"}, [])

    assert seen == {"table_name": "RAW_FAB_DATA"}


def test_executor_requires_product_table_name_instead_of_guessing(tmp_path):
    lake = RecordingLake()
    settings = {"lake_api": {"max_concurrent": 1}, "schedule": {"tolerance_pct": 0.5},
                "s3": {"enabled": False}}
    executor = ChunkExecutor(lake, None, S3Uploader(settings),
                             StateStore(tmp_path / "jobs.jsonl"), settings,
                             tmp_path / "0.STAGING")
    chunk = Chunk("P-INLINE-2026-07-31-00", "P", "INLINE", "2026-07-31")
    with pytest.raises(ValueError, match="product='P'.*source='INLINE'"):
        executor._build_params(chunk, {"params_template": {}}, {"name": "INLINE"})


def test_executor_uses_product_table_name_verbatim(tmp_path):
    lake = RecordingLake()
    settings = {"lake_api": {"max_concurrent": 1}, "schedule": {"tolerance_pct": 0.5},
                "s3": {"enabled": False}}
    executor = ChunkExecutor(lake, None, S3Uploader(settings),
                             StateStore(tmp_path / "jobs.jsonl"), settings,
                             tmp_path / "0.STAGING")
    chunk = Chunk("P-FAB-2026-07-31-00", "P", "FAB", "2026-07-31")
    params = executor._build_params(
        chunk, {"params_template": {"table_name": "wrong-template-table"}},
        {"name": "FAB", "table_name": "Product_P_Fab_Table_v7"},
    )
    assert params["table_name"] == "Product_P_Fab_Table_v7"
    assert "table" not in params


def test_disabled_s3_does_not_initialize_fake_storage(tmp_path):
    fake_root = tmp_path / "should-not-exist"
    up = S3Uploader({"s3": {"enabled": False, "bucket": "x",
                            "fake_local_path": str(fake_root)}})
    assert up.is_configured() is False
    assert not fake_root.exists()


def test_combined_view_reads_all_wide_form_files(tmp_path):
    db = tmp_path / "db"
    wide = db / "4.WIDE_FORM"
    wide.mkdir(parents=True)
    pl.DataFrame({"PRODUCT": ["A"], "ROOT_LOT_ID": ["R1"], "WAFER_ID": [1],
                  "FAB_CD": [1.0], "INLINE_THK": [2.0]}).write_parquet(
        wide / "ML_TABLE_VH_A.parquet")
    pl.DataFrame({"PRODUCT": ["B"], "ROOT_LOT_ID": ["R2"], "WAFER_ID": [2],
                  "VM_SCORE": [3.0]}).write_parquet(wide / "ML_TABLE_VH_B.parquet")
    browser.deps(tmp_path / "staging", None, extra_roots={"db": db})

    result = query.combined(root="db", sql="", rows=200)

    assert result["n_rows"] == 2
    assert set(result["files"]) == {"ML_TABLE_VH_A.parquet", "ML_TABLE_VH_B.parquet"}
    assert {"FAB_CD", "INLINE_THK", "VM_SCORE"}.issubset(result["columns"])
