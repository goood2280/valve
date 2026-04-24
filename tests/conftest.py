"""Valve pytest fixtures.

- fake_lake: 결정론적 mock lake_api (HY000/timeout 주입 X, pd.DataFrame 반환)
- flaky_lake: 호출마다 실패/성공 스크립트 주입 가능
- tmp_state: 임시 StateStore (jobs.jsonl 포함)
- sample_products / sample_settings: 최소 설정 딕셔너리
- client: FastAPI TestClient (앱 전체)
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture
def sample_settings():
    return {
        "lake_api": {
            "mode": "mock",
            "module": "",
            "user": "test",
            "timeout_sec": 10,
            "min_interval_sec": 0.0,
            "max_concurrent": 2,
            "retry": {"attempts": 1, "backoff_sec": [0]},
            "retryable_errors": ["HY000", "TimeoutError"],
        },
        "s3": {
            "endpoint_url": "",
            "bucket": "test",
            "prefix": "",
            "access_key": "",
            "secret_key": "",
            "fake_local_path": "s3_local",
        },
        "schedule": {
            "backfill_days": 2,
            "interval_hours": 0,
            "force_overwrite": False,
            "tolerance_pct": 0.5,
        },
        "probe": {
            "strategy": "sample_window",
            "window_hours": 1,
            "cache_days": 7,
            "adaptive_correction": True,
            "fallback_on_timeout": True,
        },
    }


@pytest.fixture
def sample_products():
    return {
        "products": [
            {
                "product": "PRODA",
                "enabled": True,
                "priority": 10,
                "sources": [
                    {"name": "FAB", "table": "RAW_FAB_DATA", "shard_hierarchy": [],
                     "target_chunk_rows": 500_000},
                    {"name": "INLINE", "table": "RAW_INLINE_DATA",
                     "shard_hierarchy": ["root_lot_id"], "target_chunk_rows": 200_000},
                ],
                "params_template": {
                    "product_code": {"op": "eq", "value": "PRODA"},
                },
                "custom_col": ["lot_id", "wafer_id", "time", "value"],
            },
            {
                "product": "PRODB",
                "enabled": True,
                "priority": 20,
                "sources": [
                    {"name": "FAB", "table": "RAW_FAB_DATA", "shard_hierarchy": [],
                     "target_chunk_rows": 500_000, "probe_skip": True},
                ],
                "params_template": {
                    "product_code": {"op": "eq", "value": "PRODB"},
                },
                "custom_col": ["lot_id", "wafer_id", "time"],
                "backfill_days_override": 5,
            },
        ]
    }


class FakeLakeAPI:
    """planner/executor 가 기대하는 await api.query(params, custom_col) 를 흉내낸 테스트용 LakeAPI."""

    def __init__(self, row_factory=None, raise_on=None):
        # row_factory(params, custom_col) -> pd.DataFrame (or polars OK)
        # raise_on: list of exceptions to raise per call (pop front); None → 정상
        self.row_factory = row_factory or self._default_rows
        self.raise_on = list(raise_on or [])
        self.calls = []

    @staticmethod
    def _default_rows(params, custom_col):
        # 1h 샘플이면 3000 행, 하루면 72000 행 흉내
        from datetime import datetime, timedelta
        try:
            t0 = datetime.fromisoformat(params["dateFrom"])
            t1 = datetime.fromisoformat(params["dateTo"])
            hours = max((t1 - t0).total_seconds() / 3600.0, 0.01)
        except Exception:
            hours = 24.0
        per_hour = 3000
        n = int(per_hour * hours)
        data = {}
        for col in custom_col:
            if col == "root_lot_id":
                data[col] = [f"R{i%5:03d}" for i in range(n)]
            elif col == "lot_id":
                data[col] = [f"L{i%100:04d}" for i in range(n)]
            elif col == "wafer_id":
                data[col] = [i % 25 + 1 for i in range(n)]
            elif col == "value":
                data[col] = [float(i % 100) for i in range(n)]
            elif col == "time":
                data[col] = [(t0 + timedelta(seconds=i)) for i in range(n)]
            else:
                data[col] = [f"{col}_{i%10}" for i in range(n)]
        return pd.DataFrame(data)

    async def query(self, params, custom_col):
        self.calls.append({"params": dict(params), "custom_col": list(custom_col)})
        if self.raise_on:
            exc = self.raise_on.pop(0)
            if exc is not None:
                raise exc
        import polars as pl
        df = self.row_factory(params, custom_col)
        return pl.from_pandas(df) if isinstance(df, pd.DataFrame) else df


@pytest.fixture
def fake_lake():
    return FakeLakeAPI()


@pytest.fixture
def tmp_state(tmp_path):
    from backend.core.state import StateStore
    return StateStore(tmp_path / "logs" / "jobs.jsonl")


@pytest.fixture
def tmp_planner(tmp_path, sample_settings, fake_lake):
    from backend.core.planner import Planner
    return Planner(fake_lake, sample_settings, tmp_path / "config" / "probe_cache.json")


@pytest.fixture
def app_client(tmp_path, sample_settings, sample_products):
    """전체 FastAPI app 을 일회용 ROOT 에 올린 TestClient."""
    from fastapi.testclient import TestClient
    # ROOT 를 tmp 로 맞춰야 products.yaml·source_types 가 격리됨
    os.environ["VALVE_ROOT"] = str(tmp_path)
    # staging/logs/s3_local/config 디렉터리 미리 생성
    for sub in ("config", "logs", "staging", "s3_local"):
        (tmp_path / sub).mkdir(exist_ok=True)
    import yaml
    (tmp_path / "config" / "products.yaml").write_text(
        yaml.safe_dump(sample_products, allow_unicode=True, sort_keys=False), encoding="utf-8")
    (tmp_path / "config" / "settings.json").write_text(
        json.dumps(sample_settings, indent=2), encoding="utf-8")
    # 앱 임포트는 env 설정 후에
    if "app" in sys.modules:
        del sys.modules["app"]
    import importlib
    app_module = importlib.import_module("app")
    with TestClient(app_module.app) as client:
        yield client
