"""조회 결과가 비었을 때(그 날 lot 이 없음) 파이프라인이 멀쩡해야 한다.

사내 어댑터는 '해당 조건 데이터 없음' 을 None·빈 list·빈 DataFrame 으로 준다
(lake_api._to_polars 가 전부 0행 0열 DataFrame 으로 바꾼다). 이건 **에러가 아니라
정상 결과**다 — root_lot 단위/chunk 로 쪼개 뽑으면 어떤 조각은 당연히 비어서 온다.

여기서 지키는 것:
  · 빈 결과가 없는 행을 만들어 내지 않는다 (예전엔 split 리터럴 때문에 1행이 생겼다)
  · 빈 결과가 컬럼 없는 parquet 으로 저장돼 다음 단계를 깨뜨리지 않는다
  · 빈 조각이 섞여도 event/feature/wide 가 끝까지 돈다
  · chunk 실행에서 0행은 실패가 아니다 (재시도 큐·알람으로 새지 않는다)
"""
import asyncio
import shutil
from pathlib import Path

import polars as pl
import pytest
import yaml

from backend.core.feature_pipeline import FeaturePipeline
from backend.core.planner import Chunk, ChunkPlan, Planner

REPO = Path(__file__).parent.parent


class EmptyLake:
    """무엇을 물어도 '데이터 없음' 을 주는 어댑터 (lake_api 가 만드는 모양 그대로)."""

    def __init__(self, df=None):
        self.df = pl.DataFrame() if df is None else df
        self.calls = 0

    async def query(self, params, custom_col, user=None):
        self.calls += 1
        return self.df


@pytest.fixture()
def pipe_factory(tmp_path):
    shutil.copytree(REPO / "config", tmp_path / "config")
    path = tmp_path / "config" / "pipeline.yaml"
    cfg = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    cfg["db_root"] = "db"
    cfg["runtime"]["quiet_enabled"] = False
    path.write_text(yaml.safe_dump(cfg, allow_unicode=True, sort_keys=False), encoding="utf-8")

    def make(lake):
        return FeaturePipeline(tmp_path, {}, lake_api=lake)
    return make


VEHICLE = "VH_PRODA"


# ── raw ────────────────────────────────────────────────────
def test_empty_result_does_not_invent_a_row(pipe_factory):
    pipe = pipe_factory(EmptyLake())
    stats = pipe.run_raw_query(VEHICLE)
    assert all(n == 0 for n in stats["rows"].values()), stats["rows"]

    for source in pipe.sources_cfg():
        for f in pipe.raw_dir(VEHICLE, source).glob("date=*/*.parquet"):
            df = pl.read_parquet(f)
            assert df.height == 0, f"{f} 에 없는 행이 생겼다: {df}"


def test_empty_result_writes_no_partition(pipe_factory):
    """빈 날은 파티션을 만들지 않는다 — 0행 파티션이 '최신' 이 되면 진단이 빈 날을
    실패로 읽고, 일시적인 빈 응답이 이미 받아 둔 데이터를 덮어쓴다."""
    pipe = pipe_factory(EmptyLake())
    pipe.run_raw_query(VEHICLE)
    for source in pipe.sources_cfg():
        assert list(pipe.raw_dir(VEHICLE, source).glob("date=*")) == []


def test_normalized_empty_frame_keeps_schema_and_zero_rows(pipe_factory):
    """혹시 저장되더라도 컬럼 없는 parquet 은 안 된다 — 다음 단계가
    root_lot_id 를 못 찾아 ColumnNotFoundError 로 터진다."""
    pipe = pipe_factory(EmptyLake())
    df = pipe._normalize_raw(pl.DataFrame(), "FAB", "S1")
    assert df.height == 0                       # 리터럴이 없는 행을 만들지 않는다
    assert "root_lot_id" in df.columns and "split" in df.columns
    for want in pipe.sources_cfg()["FAB"]["columns"]:
        assert want in df.columns


def test_normalize_keeps_real_rows_and_adds_split(pipe_factory):
    pipe = pipe_factory(EmptyLake())
    df = pipe._normalize_raw(pl.DataFrame({"root_lot_id": ["R1"]}), "FAB", "S1")
    assert df.height == 1 and df["split"][0] == "S1"


def test_empty_root_lot_probe_does_not_run_unfiltered_query(pipe_factory):
    from datetime import date

    lake = EmptyLake()
    pipe = pipe_factory(lake)
    df = pipe._query_raw(
        pipe.vehicle_cfg(VEHICLE), "INLINE", date(2026, 8, 1), date(2026, 8, 2)
    )

    assert df.height == 0
    assert lake.calls == 1


# ── event / feature / wide ─────────────────────────────────
def test_whole_run_is_not_a_failure_when_there_is_no_data(pipe_factory):
    """전 구간이 비어도 실행 한 회차가 성공으로 끝나야 한다 —
    예전엔 run_feature 가 RuntimeError 를 내서 매 회차가 critical 알람이었다."""
    from backend.core.pipeline_runner import PipelineRunner
    pipe = pipe_factory(EmptyLake())
    out = PipelineRunner(pipe).run_vehicle(VEHICLE)
    assert out["errors"] == []
    assert out["feature"] == {}
    assert "보존" in out["wide"]["skipped"]      # 이전 ML_TABLE 을 지우지 않는다


def test_event_survives_an_empty_partition_among_real_ones(pipe_factory):
    """빈 조각이 섞여 있어도 나머지 날짜는 그대로 만들어져야 한다 —
    예전엔 컬럼 없는 파티션 하나가 그 제품 event 단계를 통째로 죽였다."""
    pipe = pipe_factory(EmptyLake())
    src = "FAB"
    cols = pipe.sources_cfg()[src]["columns"]
    step = pipe.step_map(VEHICLE)["step_id"][0]
    row = {c: "x" for c in cols}
    row.update({"root_lot_id": "R001", "wafer_id": "1", "step_id": step,
                "tkout_time": "2026-08-01 01:00:00"})
    root = pipe.raw_dir(VEHICLE, src)
    (root / "date=2026-08-01").mkdir(parents=True)
    pl.DataFrame([row]).with_columns(pl.lit("S1").alias("split")).write_parquet(
        root / "date=2026-08-01" / "data.parquet")
    # 옛 빌드가 남겼을 법한 컬럼 없는 빈 파티션
    (root / "date=2026-08-02").mkdir(parents=True)
    pl.DataFrame().write_parquet(root / "date=2026-08-02" / "data.parquet")

    ev = pipe.run_event(VEHICLE)
    assert ev[src]["event_rows"] == 1, ev
    assert ev[src]["empty_partitions"] == ["2026-08-02"]


# ── chunk 실행 (직접 쿼리 경로) ─────────────────────────────
def test_planner_makes_single_chunk_when_probe_finds_nothing(tmp_path, sample_settings,
                                                             sample_products):
    planner = Planner(EmptyLake(), sample_settings, tmp_path / "probe.json")
    prod = sample_products["products"][0]
    src = next(s for s in prod["sources"] if s["name"] == "INLINE")   # shard 있는 소스
    plan = asyncio.run(planner.build_plan(prod["product"], src, prod, "2026-08-01"))
    assert len(plan.chunks) == 1
    assert plan.chunks[0].expected_rows == 0
    assert plan.chunks[0].shard_filters == {"root_lot_id": []}


def test_empty_chunks_are_success_not_completeness_failure(tmp_path, sample_settings,
                                                           sample_products, tmp_state):
    """probe 캐시가 옛날 값이면 expected 는 큰데 실제는 0행일 수 있다.
    lot 이 없어서 빈 것을 '불완전' 으로 처리하면 재시도 큐와 알람이 계속 울린다."""
    from backend.core.executor import ChunkExecutor
    lake = EmptyLake()
    planner = Planner(lake, sample_settings, tmp_path / "probe.json")
    ex = ChunkExecutor(lake, planner, None, tmp_state, sample_settings,
                       tmp_path / "staging", db_root=tmp_path / "db")
    prod = sample_products["products"][0]
    src = prod["sources"][0]
    plan = ChunkPlan(plan_id="p", product=prod["product"], source=src["name"],
                     date="2026-08-01", probe_meta={},
                     chunks=[Chunk(chunk_id="c0", product=prod["product"],
                                   source=src["name"], date="2026-08-01",
                                   expected_rows=500_000)])
    out = asyncio.run(ex.run_plan(plan, prod, src))
    assert out["ok"] is True, out
    assert out["rows"] == 0
    assert out.get("empty") is True

    part = tmp_state.snapshot()["partitions"]["PRODA/FAB/2026-08-01"]
    assert part["status"] == "success" and part.get("empty") is True


def test_empty_direct_query_does_not_write_schemaless_db_parquet(tmp_path, sample_settings,
                                                                 sample_products, tmp_state):
    """컬럼 없는 parquet 을 1.RAWDATA_DB 에 떨구면 그 제품의 event 단계가 통째로 죽는다."""
    from backend.core.executor import ChunkExecutor
    lake = EmptyLake()
    planner = Planner(lake, sample_settings, tmp_path / "probe.json")
    ex = ChunkExecutor(lake, planner, None, tmp_state, sample_settings,
                       tmp_path / "staging", db_root=tmp_path / "db")
    prod = sample_products["products"][0]
    src = prod["sources"][0]
    plan = ChunkPlan(plan_id="p", product=prod["product"], source=src["name"],
                     date="2026-08-01", probe_meta={},
                     chunks=[Chunk(chunk_id="c0", product=prod["product"],
                                   source=src["name"], date="2026-08-01")])
    asyncio.run(ex.run_plan(plan, prod, src))

    for f in (tmp_path / "db").rglob("data.parquet"):
        assert pl.read_parquet(f).width > 0, f"{f} 가 컬럼 없는 parquet 이다"
