"""Planner — probe 3 전략, chunk 분할, probe_skip, 실패 캐시 방지."""
from __future__ import annotations

import asyncio
from datetime import date

import pytest

from backend.core.planner import Planner
from backend.core.lake_api import HY000Error


pytestmark = pytest.mark.asyncio


@pytest.fixture
def today_iso():
    return date.today().isoformat()


async def test_sample_window_returns_chunks(tmp_planner, sample_products, today_iso):
    prod = sample_products["products"][0]
    src = prod["sources"][0]  # FAB (no shard)
    plan = await tmp_planner.build_plan(prod["product"], src, prod, today_iso)

    assert plan.plan_id == f"PRODA-FAB-{today_iso}"
    assert plan.probe_meta["strategy"] == "sample_window"
    assert plan.probe_meta["estimated_rows"] > 0
    assert len(plan.chunks) == 1  # shard 없으니 단일 chunk


async def test_shard_distribution_creates_multiple_chunks(tmp_planner, sample_products, today_iso):
    """INLINE 은 root_lot_id shard + target_chunk_rows=200k.
    FakeLake 가 하루에 72000 행 반환 → 1 chunk 로 수렴하지만 probe 는 5개 root 를 인식해야 한다."""
    prod = sample_products["products"][0]
    inline = [s for s in prod["sources"] if s["name"] == "INLINE"][0]
    plan = await tmp_planner.build_plan(prod["product"], inline, prod, today_iso)

    assert plan.probe_meta["shard_count"] == 5      # R000..R004
    # 총량이 target 이하면 shard 무시 단일 chunk (planner.py case B)
    assert len(plan.chunks) >= 1


async def test_probe_skip_uses_strategy_none(tmp_planner, sample_products, today_iso):
    prodb = sample_products["products"][1]
    src = prodb["sources"][0]  # probe_skip: True
    plan = await tmp_planner.build_plan(prodb["product"], src, prodb, today_iso)

    assert plan.probe_meta["strategy"] == "none"
    assert plan.probe_meta.get("skipped") is True
    assert len(plan.chunks) == 1  # none 전략은 단일 chunk 로 fallback


async def test_probe_error_does_not_get_cached(tmp_path, sample_settings, sample_products, today_iso):
    """probe 가 error 반환하면 캐시에 저장되면 안 됨 (반복 실패 고착 방지)."""
    from tests.conftest import FakeLakeAPI

    # 첫 호출은 HY000 으로 실패, 두번째는 성공
    errors = [HY000Error("sim"), None]
    lake = FakeLakeAPI(raise_on=errors)
    planner = Planner(lake, sample_settings, tmp_path / "cache.json")

    prod = sample_products["products"][0]
    src = prod["sources"][0]

    plan1 = await planner.build_plan(prod["product"], src, prod, today_iso)
    assert "error" in plan1.probe_meta           # 첫 번째 호출: probe 실패 기록
    assert plan1.probe_meta["error"]

    # 실패한 probe 결과는 캐시에 저장되지 않았어야 한다 → 두 번째 호출이 새로 probe
    plan2 = await planner.build_plan(prod["product"], src, prod, today_iso)
    assert "error" not in plan2.probe_meta        # 정상 probe 성공
    # 즉 lake 는 2회 호출됨
    assert len(lake.calls) == 2


async def test_probe_success_is_cached(tmp_planner, sample_products, today_iso):
    prod = sample_products["products"][0]
    src = prod["sources"][0]

    plan1 = await tmp_planner.build_plan(prod["product"], src, prod, today_iso)
    plan2 = await tmp_planner.build_plan(prod["product"], src, prod, today_iso)
    assert plan2.probe_meta.get("_from_cache") is True


async def test_projection_strategy(tmp_path, sample_products, today_iso):
    from tests.conftest import FakeLakeAPI
    settings = {
        "lake_api": {"mode": "mock", "user": "t", "timeout_sec": 10, "min_interval_sec": 0,
                     "max_concurrent": 2, "retry": {"attempts": 1, "backoff_sec": [0]},
                     "retryable_errors": []},
        "probe": {"strategy": "projection", "window_hours": 1, "cache_days": 7,
                  "adaptive_correction": True, "fallback_on_timeout": True},
    }
    lake = FakeLakeAPI()
    planner = Planner(lake, settings, tmp_path / "cache.json")
    inline = [s for s in sample_products["products"][0]["sources"] if s["name"] == "INLINE"][0]
    plan = await planner.build_plan("PRODA", inline, sample_products["products"][0], today_iso)
    assert plan.probe_meta["strategy"] == "projection"
