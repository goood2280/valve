"""FabScanner 단위 테스트.

테스트 항목:
  - main_step_only 필터 (fnmatch 패턴)
  - missing steps 탐지
  - unmatched PPIDs 탐지
  - scan_ignore 필터링 (step / ppid)
  - scan_config 로드 · 저장 (config/fab_scan/{vehicle}/)
  - scan_ignore 로드 · 저장
  - scan_result.json summary 필드 검증
  - S3 발행 확인
  - run_all 전 vehicle 실행
"""
from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest
import yaml

from backend.core.fab_scanner import (
    DEFAULT_SCAN_CFG,
    FabDbClient,
    FabScanner,
)


# ── helpers ───────────────────────────────────


class FakeS3:
    """S3Uploader 최소 mock."""

    def __init__(self):
        self.store: dict[str, str] = {}

    def put_text(self, key: str, text: str) -> bool:
        self.store[key] = text
        return True


class FakePipeline:
    """FeaturePipeline 최소 mock — FabScanner 가 사용하는 메서드만 구현."""

    def __init__(self, root: Path, vehicles_map: dict, step_map_data: dict, knob_data: pl.DataFrame | None):
        self._root = root
        self._vehicles = vehicles_map
        self._step_maps = step_map_data        # {vehicle: pl.DataFrame}
        self._knob = knob_data

    def vehicles(self) -> dict:
        return self._vehicles

    def vehicle_cfg(self, vehicle: str) -> dict:
        if vehicle not in self._vehicles:
            raise ValueError(f"unknown vehicle: {vehicle}")
        cfg = dict(self._vehicles[vehicle])
        cfg["vehicle"] = vehicle
        return cfg

    def step_map(self, vehicle: str) -> pl.DataFrame:
        return self._step_maps.get(vehicle, pl.DataFrame({"step_id": [], "step_desc": []}))

    def rules_csv(self, name: str) -> pl.DataFrame | None:
        if name == "knob":
            return self._knob
        return None

    def knob_map(self, vehicle: str) -> pl.DataFrame | None:
        if self._knob is None:
            return None
        return self._knob.filter(pl.col("vehicle") == vehicle)

    def report_dir(self, vehicle: str) -> Path:
        d = self._root / "db" / "REPORTS" / vehicle
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _load_raw(self, vehicle: str, source: str) -> pl.DataFrame | None:
        return None


class FixedDbClient(FabDbClient):
    """테스트용 고정 DataFrame 반환 클라이언트."""

    def __init__(self, data: pl.DataFrame):
        self._data = data

    def query_step_data(self, vehicle_cfg: dict, scan_cfg: dict) -> pl.DataFrame:
        return self._data


# ── fixtures ──────────────────────────────────


@pytest.fixture
def tmp_project(tmp_path):
    """최소 프로젝트 구조 생성."""
    (tmp_path / "config" / "fab_scan" / "VH_TEST").mkdir(parents=True)
    (tmp_path / "db" / "REPORTS" / "VH_TEST").mkdir(parents=True)
    return tmp_path


@pytest.fixture
def step_map_df():
    """vehicle_matching 에 해당하는 step_map."""
    return pl.DataFrame({
        "step_id": ["CC100", "CC200", "CC300"],
        "step_desc": ["ETCH_A", "CVD_B", "CLEAN_C"],
    })


@pytest.fixture
def knob_df():
    """knob_ppid.csv mock."""
    return pl.DataFrame({
        "vehicle": ["VH_TEST", "VH_TEST", "VH_TEST"],
        "step_id": ["CC100", "CC100", "CC200"],
        "step_desc": ["ETCH_A", "ETCH_A", "CVD_B"],
        "ppid": ["PP_A1", "PP_A2", "PP_B1"],
        "knob": ["KNOB_1", "KNOB_2", "KNOB_3"],
    })


@pytest.fixture
def fab_data():
    """FAB DB 에서 온 것처럼 보이는 raw 데이터.

    - CC100, CC200 은 matched (step_map 에 있음)
    - CC999 는 missing
    - CC100 에 PP_NEW (knob 미등록 PPID) 포함
    """
    return pl.DataFrame({
        "step_id":      ["CC100", "CC100", "CC100", "CC200", "CC999", "CC999"],
        "root_lot_id":  ["R001",  "R001",  "R002",  "R001",  "R003",  "R004"],
        "wafer_id":     ["1",     "2",     "1",     "1",     "1",     "1"],
        "ppid":         ["PP_A1", "PP_NEW","PP_A1", "PP_B1", "PP_X",  "PP_X"],
        "eqp_id":       ["E01",   "E01",   "E02",  "E01",   "E03",   "E04"],
        "step_desc":    ["ETCH",  "ETCH",  "ETCH",  "CVD",  "MAIN",  "MAIN"],
        "eqp_model":    ["GEN-1", "GEN-1", "GEN-1", "GEN-1","GEN-1", "GEN-1"],
    })


def _make_scanner(tmp_project, step_map_df, knob_df, fab_data):
    """공통 scanner 생성 헬퍼."""
    vehicles_map = {"VH_TEST": {"product": "TEST", "process_id": "P1"}}
    pipe = FakePipeline(tmp_project, vehicles_map, {"VH_TEST": step_map_df}, knob_df)
    db = FixedDbClient(fab_data)
    s3 = FakeS3()
    settings = {"alerts": {"s3_prefix": "valve-alerts"}}
    scanner = FabScanner(tmp_project, pipe, db, s3, settings)
    return scanner, s3


# ── main_step_only 필터 ──────────────────────


class TestMainStepFilter:
    """_apply_main_step_filter 단위 테스트."""

    def test_excludes_measure_steps(self):
        df = pl.DataFrame({
            "step_id": ["S1", "S2", "S3"],
            "step_desc": ["ETCH_GATE", "MEASURE_CD", "AUX_CLEAN"],
            "eqp_model": ["GEN-1", "GEN-1", "GEN-1"],
        })
        out = FabScanner._apply_main_step_filter(df, {
            "step_desc": ["MEASURE*", "AUX*"],
        })
        assert out["step_id"].to_list() == ["S1"]

    def test_excludes_eqp_model_patterns(self):
        df = pl.DataFrame({
            "step_id": ["S1", "S2", "S3"],
            "step_desc": ["A", "B", "C"],
            "eqp_model": ["GEN-1", "MEA-100", "SEM-200"],
        })
        out = FabScanner._apply_main_step_filter(df, {
            "eqp_model": ["MEA-*", "SEM-*"],
        })
        assert out["step_id"].to_list() == ["S1"]

    def test_no_exclude_returns_all(self):
        df = pl.DataFrame({
            "step_id": ["S1", "S2"],
            "step_desc": ["A", "B"],
        })
        out = FabScanner._apply_main_step_filter(df, {})
        assert out.height == 2

    def test_missing_columns_no_error(self):
        """step_desc / eqp_model 컬럼이 없어도 에러 나지 않는다."""
        df = pl.DataFrame({"step_id": ["S1", "S2"]})
        out = FabScanner._apply_main_step_filter(df, {
            "step_desc": ["MEASURE*"],
            "eqp_model": ["MEA-*"],
        })
        assert out.height == 2


# ── missing steps ────────────────────────────


class TestMissingSteps:

    def test_finds_missing_step(self, tmp_project, step_map_df, knob_df, fab_data):
        scanner, _ = _make_scanner(tmp_project, step_map_df, knob_df, fab_data)
        result = scanner.run("VH_TEST")
        missing_ids = [m["step_id"] for m in result["missing_steps"]]
        assert "CC999" in missing_ids
        # CC100, CC200 은 missing 이 아님
        assert "CC100" not in missing_ids
        assert "CC200" not in missing_ids

    def test_missing_has_lot_count(self, tmp_project, step_map_df, knob_df, fab_data):
        scanner, _ = _make_scanner(tmp_project, step_map_df, knob_df, fab_data)
        result = scanner.run("VH_TEST")
        cc999 = [m for m in result["missing_steps"] if m["step_id"] == "CC999"][0]
        # CC999 에 R003, R004 두 lot
        assert cc999["lot_count"] == 2
        assert len(cc999["hits"]) > 0


# ── unmatched PPIDs ──────────────────────────


class TestUnmatchedPpids:

    def test_finds_unmatched_ppid(self, tmp_project, step_map_df, knob_df, fab_data):
        scanner, _ = _make_scanner(tmp_project, step_map_df, knob_df, fab_data)
        result = scanner.run("VH_TEST")
        um = result["unmatched_ppids"]
        ppid_pairs = [(u["step_id"], u["ppid"]) for u in um]
        assert ("CC100", "PP_NEW") in ppid_pairs

    def test_existing_splits_included(self, tmp_project, step_map_df, knob_df, fab_data):
        scanner, _ = _make_scanner(tmp_project, step_map_df, knob_df, fab_data)
        result = scanner.run("VH_TEST")
        pp_new = [u for u in result["unmatched_ppids"]
                  if u["step_id"] == "CC100" and u["ppid"] == "PP_NEW"][0]
        assert "PP_A1" in pp_new["existing_splits"]
        assert "PP_A2" in pp_new["existing_splits"]


# ── scan_ignore ──────────────────────────────


class TestScanIgnore:

    def test_ignore_step_filters_missing(self, tmp_project, step_map_df, knob_df, fab_data):
        scanner, _ = _make_scanner(tmp_project, step_map_df, knob_df, fab_data)
        # CC999 를 ignore 로 등록
        scanner.save_scan_ignore("VH_TEST", [
            {"type": "step", "key": "CC999", "reason": "test ignore"},
        ])
        result = scanner.run("VH_TEST")
        missing_ids = [m["step_id"] for m in result["missing_steps"]]
        assert "CC999" not in missing_ids

    def test_ignore_ppid_filters_unmatched(self, tmp_project, step_map_df, knob_df, fab_data):
        scanner, _ = _make_scanner(tmp_project, step_map_df, knob_df, fab_data)
        scanner.save_scan_ignore("VH_TEST", [
            {"type": "ppid", "key": "CC100:PP_NEW", "reason": "test ppid ignore"},
        ])
        result = scanner.run("VH_TEST")
        ppid_pairs = [(u["step_id"], u["ppid"]) for u in result["unmatched_ppids"]]
        assert ("CC100", "PP_NEW") not in ppid_pairs

    def test_save_and_load_ignore(self, tmp_project, step_map_df, knob_df, fab_data):
        scanner, _ = _make_scanner(tmp_project, step_map_df, knob_df, fab_data)
        items = [
            {"type": "step", "key": "XX1", "reason": "r1"},
            {"type": "ppid", "key": "CC100:PP_X", "reason": "r2"},
        ]
        saved = scanner.save_scan_ignore("VH_TEST", items)
        assert len(saved) == 2
        loaded = scanner.scan_ignore("VH_TEST")
        assert loaded == saved

    def test_save_ignore_cleans_empty_keys(self, tmp_project, step_map_df, knob_df, fab_data):
        scanner, _ = _make_scanner(tmp_project, step_map_df, knob_df, fab_data)
        items = [
            {"type": "step", "key": "", "reason": "no key"},
            {"type": "step", "key": "XX1", "reason": "ok"},
        ]
        saved = scanner.save_scan_ignore("VH_TEST", items)
        assert len(saved) == 1
        assert saved[0]["key"] == "XX1"


# ── config ───────────────────────────────────


class TestScanConfig:

    def test_default_config(self, tmp_project, step_map_df, knob_df, fab_data):
        scanner, _ = _make_scanner(tmp_project, step_map_df, knob_df, fab_data)
        cfg = scanner.scan_config("VH_TEST")
        assert cfg["vehicle"] == "VH_TEST"
        assert cfg["main_step_only"] is True
        assert "main_step_exclude" in cfg

    def test_save_and_reload_config(self, tmp_project, step_map_df, knob_df, fab_data):
        scanner, _ = _make_scanner(tmp_project, step_map_df, knob_df, fab_data)
        scanner.save_scan_config("VH_TEST", {
            "eqp_filter": ["EQP_A01"],
            "max_hits": 5,
            "main_step_only": False,
        })
        cfg = scanner.scan_config("VH_TEST")
        assert cfg["eqp_filter"] == ["EQP_A01"]
        assert cfg["max_hits"] == 5
        assert cfg["main_step_only"] is False

    def test_config_dir_is_fab_scan(self, tmp_project, step_map_df, knob_df, fab_data):
        scanner, _ = _make_scanner(tmp_project, step_map_df, knob_df, fab_data)
        d = scanner.scan_dir("VH_TEST")
        assert "fab_scan" in str(d)
        assert d == tmp_project / "config" / "fab_scan" / "VH_TEST"


# ── result + S3 발행 ─────────────────────────


class TestResultAndPublish:

    def test_result_has_summary(self, tmp_project, step_map_df, knob_df, fab_data):
        scanner, _ = _make_scanner(tmp_project, step_map_df, knob_df, fab_data)
        result = scanner.run("VH_TEST")
        assert "summary" in result
        s = result["summary"]
        assert s["total_fab_rows"] > 0
        assert s["matching_steps_count"] == 3
        assert s["missing_count"] == len(result["missing_steps"])
        assert s["unmatched_count"] == len(result["unmatched_ppids"])
        assert "ignored" in s

    def test_result_saved_to_reports(self, tmp_project, step_map_df, knob_df, fab_data):
        scanner, _ = _make_scanner(tmp_project, step_map_df, knob_df, fab_data)
        scanner.run("VH_TEST")
        path = tmp_project / "db" / "REPORTS" / "VH_TEST" / "scan_result.json"
        assert path.exists()
        data = json.loads(path.read_text(encoding="utf-8"))
        assert data["vehicle"] == "VH_TEST"

    def test_result_published_to_s3(self, tmp_project, step_map_df, knob_df, fab_data):
        scanner, s3 = _make_scanner(tmp_project, step_map_df, knob_df, fab_data)
        scanner.run("VH_TEST")
        key = "valve-alerts/scan/VH_TEST.json"
        assert key in s3.store
        data = json.loads(s3.store[key])
        assert data["vehicle"] == "VH_TEST"

    def test_last_result_returns_saved(self, tmp_project, step_map_df, knob_df, fab_data):
        scanner, _ = _make_scanner(tmp_project, step_map_df, knob_df, fab_data)
        scanner.run("VH_TEST")
        last = scanner.last_result("VH_TEST")
        assert last is not None
        assert last["vehicle"] == "VH_TEST"

    def test_last_result_none_when_no_scan(self, tmp_project, step_map_df, knob_df, fab_data):
        scanner, _ = _make_scanner(tmp_project, step_map_df, knob_df, fab_data)
        assert scanner.last_result("VH_NEVER") is None


# ── empty data ───────────────────────────────


class TestEmptyData:

    def test_empty_fab_data(self, tmp_project, step_map_df, knob_df):
        """FAB 데이터 없으면 빈 결과 + summary."""
        empty = pl.DataFrame({
            "step_id": [], "root_lot_id": [], "wafer_id": [],
            "ppid": [], "eqp_id": [],
        })
        vehicles_map = {"VH_TEST": {"product": "TEST"}}
        pipe = FakePipeline(tmp_project, vehicles_map, {"VH_TEST": step_map_df}, knob_df)
        db = FixedDbClient(empty)
        s3 = FakeS3()
        scanner = FabScanner(tmp_project, pipe, db, s3, {"alerts": {}})
        result = scanner.run("VH_TEST")
        assert result["missing_steps"] == []
        assert result["unmatched_ppids"] == []
        assert result["summary"]["total_fab_rows"] == 0


# ── run_all ──────────────────────────────────


class TestRunAll:

    def test_run_all_returns_all_vehicles(self, tmp_project, step_map_df, knob_df, fab_data):
        vehicles_map = {
            "VH_TEST": {"product": "TEST"},
            "VH_OTHR": {"product": "OTHR"},
        }
        pipe = FakePipeline(
            tmp_project, vehicles_map,
            {"VH_TEST": step_map_df, "VH_OTHR": step_map_df},
            knob_df,
        )
        db = FixedDbClient(fab_data)
        s3 = FakeS3()
        scanner = FabScanner(tmp_project, pipe, db, s3, {"alerts": {}})
        results = scanner.run_all()
        assert "VH_TEST" in results
        assert "VH_OTHR" in results
