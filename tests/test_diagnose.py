"""diagnose — 사내 반입 시 "어디서 막혔는지" 를 단계별로 짚어 주는지 검증.

핵심 계약 두 가지:
  · 앞 단계가 실패해도 뒤 단계까지 전부 검사한다 (어디까지 되는지 한 번에 봐야 한다)
  · 검사마다 열어볼 parquet(view)을 같이 준다 (숫자만 보여주고 끝내지 않는다)
"""
import shutil
from pathlib import Path

import polars as pl
import pytest
import yaml

from backend.core.diagnose import diagnose
from backend.core.feature_pipeline import FeaturePipeline

REPO = Path(__file__).parent.parent


@pytest.fixture()
def pipe(tmp_path):
    shutil.copytree(REPO / "config", tmp_path / "config")
    path = tmp_path / "config" / "pipeline.yaml"
    cfg = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    cfg["db_root"] = "db"
    cfg["runtime"]["quiet_enabled"] = False
    path.write_text(yaml.safe_dump(cfg, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return FeaturePipeline(tmp_path, {})


def _stage(d, key):
    return next(s for s in d["stages"] if s["key"] == key)


def _check(stage, name_part):
    return next(c for c in stage["checks"] if name_part in c["name"])


def test_empty_install_blocks_at_raw_but_still_checks_everything(pipe):
    d = diagnose(pipe, "VH_PRODA")
    assert [s["key"] for s in d["stages"]] == ["raw", "event", "feature", "send", "s3"]
    assert d["blocked_at"] == "raw"
    # 앞이 막혀도 뒤 단계 검사를 건너뛰지 않는다
    assert _stage(d, "feature")["checks"]
    assert _check(_stage(d, "feature"), "ML_TABLE (flow 발행본)")["status"] == "fail"


def test_mock_mode_is_reported_as_failure(pipe):
    """lake_api 미연결 = mock 합성 데이터. 사내에서 이게 조용히 흐르면 안 된다."""
    c = _check(_stage(diagnose(pipe, "VH_PRODA"), "raw"), "조회 모드")
    assert c["status"] == "fail" and "mock" in c["detail"]
    assert c["fix"]


def test_full_mock_run_passes_every_stage_except_mode(pipe):
    pipe.run_raw_query("VH_PRODA")
    pipe.run_event("VH_PRODA")
    pipe.run_feature("VH_PRODA")
    pipe.run_wide("VH_PRODA")
    pipe.run_flow_tables()

    d = diagnose(pipe, "VH_PRODA")
    raw = _stage(d, "raw")
    # 조회 모드(mock) 하나만 실패로 남아야 한다 — 나머지 raw 검사는 통과
    assert [c["name"] for c in raw["checks"] if c["status"] == "fail"] == ["조회 모드"]
    ev = _stage(d, "event")
    assert ev["failed"] == 0, [c for c in ev["checks"] if c["status"] == "fail"]
    # 데모 데이터에는 미매칭 step 이 일부러 섞여 있다 — 막힌 게 아니라 '확인' 이다
    assert _check(ev, "FAB step 매칭률")["status"] == "warn"
    feat = _stage(d, "feature")
    assert _check(feat, "ML_TABLE (flow 발행본)")["status"] == "ok"


def test_send_form_prefix_split_is_checked(pipe):
    """최종 납품물은 wide 가 아니라 prefix 로 쪼갠 SEND_FORM 이다 — 여기까지 봐야 한다."""
    pipe.run_raw_query("VH_PRODA")
    pipe.run_event("VH_PRODA")
    pipe.run_feature("VH_PRODA")
    pipe.run_wide("VH_PRODA")

    send = _stage(diagnose(pipe, "VH_PRODA"), "send")
    assert send["status"] == "fail"          # wide 는 있는데 분리가 안 됐다
    knob = _check(send, "0.KNOB")
    assert "없습니다" in knob["detail"] and knob["fix"]

    pipe.run_send_form()
    send = _stage(diagnose(pipe, "VH_PRODA"), "send")
    assert send["failed"] == 0, [c for c in send["checks"] if c["status"] == "fail"]
    for group in ("0.KNOB", "1.FAB", "2.VM", "3.INLINE"):
        c = _check(send, group)
        assert c["status"] == "ok" and "csv 있음" in c["detail"]
        assert c["view"]["file"].startswith("5.SEND_FORM/")


def test_send_form_missing_columns_are_named(pipe):
    """wide 에 있는 prefix 컬럼이 분리본에서 빠지면 조용히 넘어가면 안 된다."""
    import polars as pl
    pipe.run_raw_query("VH_PRODA")
    pipe.run_event("VH_PRODA")
    pipe.run_feature("VH_PRODA")
    pipe.run_wide("VH_PRODA")
    pipe.run_send_form()

    out = pipe.send_dir() / "1.FAB" / "FAB_ML_TABLE.parquet"
    df = pl.read_parquet(out)
    dropped = [c for c in df.columns if c.startswith("FAB_")][0]
    df.drop(dropped).write_parquet(out)

    c = _check(_stage(diagnose(pipe, "VH_PRODA"), "send"), "1.FAB")
    assert c["status"] == "fail" and dropped in c["detail"]


def test_every_data_check_carries_a_viewer_target(pipe):
    """진단 결과에서 바로 parquet 을 열 수 있어야 한다 (탐색기 root/file/sql)."""
    pipe.run_raw_query("VH_PRODA")
    pipe.run_event("VH_PRODA")
    d = diagnose(pipe, "VH_PRODA")
    views = [c["view"] for s in d["stages"] for c in s["checks"] if c["view"]]
    assert views
    for v in views:
        assert v["root"] in ("db", "config") and v["file"] and v["label"]
        assert not Path(v["file"]).is_absolute() and ".." not in v["file"]
    # raw·event 각각 최소 하나는 열어볼 수 있어야 의미가 있다
    assert any(v["file"].startswith("1.RAWDATA_DB") for v in views)
    assert any(v["file"].startswith("2.EVENT_DB") for v in views)


def test_wrong_lot_prefix_is_pinpointed(pipe):
    """event 가 0행인 흔한 원인 — root_lot prefix 가 실제 lot 체계와 다르다."""
    pipe.run_raw_query("VH_PRODA")
    veh = pipe.vehicles()
    veh["VH_PRODA"]["event_lot_startwith"] = "ZZZZ"
    pipe.save_vehicles(veh)
    pipe.run_event("VH_PRODA")

    c = _check(_stage(diagnose(pipe, "VH_PRODA"), "event"), "FAB root_lot prefix")
    assert c["status"] == "fail" and "ZZZZ" in c["fix"]
    assert c["view"]["file"].startswith("1.RAWDATA_DB/FAB")


def test_unmatched_steps_lower_the_match_rate(pipe):
    """매칭 테이블에서 step 을 지우면 매칭률 검사가 그걸 짚는다."""
    pipe.run_raw_query("VH_PRODA")
    mf = pipe.matching_file("FAB")
    rows = pl.read_csv(mf, infer_schema_length=0)
    rows.head(1).write_csv(mf)      # step 하나만 남긴다
    pipe.run_event("VH_PRODA")

    c = _check(_stage(diagnose(pipe, "VH_PRODA"), "event"), "FAB step 매칭률")
    assert c["status"] in ("warn", "fail")
    assert "미매칭 예" in c["detail"] and c["fix"]


def test_missing_matching_download_item_is_flagged(pipe):
    """매칭 csv 를 내려받는 S3 항목이 없으면 룰북이 flow 판정을 못 받는다."""
    c = _check(_stage(diagnose(pipe, "VH_PRODA"), "feature"), "매칭 테이블 S3 다운로드")
    assert c["status"] == "fail" and c["fix"]


def test_s3_download_status_is_surfaced(pipe):
    """등록된 다운로드 항목의 마지막 결과를 그대로 보여준다."""
    class FakeJobs:
        def list_with_status(self):
            return {"auto_download_enabled": True, "items": [
                {"id": "matching_vehicle", "direction": "download", "s3_configured": True,
                 "key": "flow/artifacts/matching/Vehicle_matching.csv",
                 "status": {"last_status": "error", "last_end": 1.0, "error": "AccessDenied"}},
                {"id": "matching_knob", "direction": "download", "s3_configured": True,
                 "key": "flow/artifacts/matching/ppid_knob.csv",
                 "status": {"last_status": "ok", "last_end": 1.0}},
            ]}

    feat = _stage(diagnose(pipe, "VH_PRODA", s3_jobs=FakeJobs()), "feature")
    bad = _check(feat, "S3 다운로드 [matching_vehicle]")
    good = _check(feat, "S3 다운로드 [matching_knob]")
    assert bad["status"] == "fail" and "AccessDenied" in bad["detail"]
    assert good["status"] == "ok"


def test_s3_upload_card_separates_transport_from_production(pipe):
    """만들어 놓고 안 보내면 flow 는 옛 데이터를 본다 — 전송은 산출과 다른 고장이다."""
    import time

    class FakeJobs:
        def list_with_status(self):
            return {"auto_download_enabled": True, "auto_upload_enabled": True, "items": [
                {"id": "up_ml_PRODA", "direction": "upload", "root": "db",
                 "target": "ML_TABLE_PRODA.parquet", "key": "flow/artifacts/ml-tables/x",
                 "interval_min": 10, "enabled": True, "s3_configured": True,
                 "status": {"last_status": "ok", "last_end": time.time()}},
                {"id": "up_outbox", "direction": "upload", "root": "outbox", "target": "",
                 "key": "valve-alerts", "interval_min": 10, "enabled": True,
                 "s3_configured": True,
                 "status": {"last_status": "error", "last_end": 1.0, "error": "AccessDenied"}},
                {"id": "up_manual", "direction": "upload", "root": "db", "target": "",
                 "key": "valve-export/db", "interval_min": 0, "enabled": True,
                 "s3_configured": True, "status": {}},
            ]}

        def _resolve(self, root, target):
            return pipe.db_root() / target

    s3 = _stage(diagnose(pipe, "VH_PRODA", s3_jobs=FakeJobs()), "s3")
    assert _check(s3, "up_ml_PRODA")["status"] == "ok"
    bad = _check(s3, "up_outbox")
    assert bad["status"] == "fail" and "AccessDenied" in bad["detail"] and bad["fix"]
    assert _check(s3, "up_manual")["status"] == "skip"   # 수동 전용은 고장이 아니다


def test_s3_upload_card_flags_auto_upload_off(pipe):
    class FakeJobs:
        def list_with_status(self):
            return {"auto_upload_enabled": False, "auto_download_enabled": True, "items": [
                {"id": "up_ml", "direction": "upload", "root": "db", "target": "x.parquet",
                 "key": "k", "interval_min": 10, "enabled": True, "s3_configured": True,
                 "status": {"last_status": "ok", "last_end": 1.0}}]}

        def _resolve(self, root, target):
            return pipe.db_root() / target

    c = _check(_stage(diagnose(pipe, "VH_PRODA", s3_jobs=FakeJobs()), "s3"), "자동 업로드")
    assert c["status"] == "fail" and c["fix"]


def test_broken_time_col_config_is_reported_not_raised(pipe):
    """기준 열이 조회 컬럼에 없으면 하루치가 엉뚱한 파티션에 들어간다 — 진단이 죽지 않고 짚는다."""
    pipe.run_raw_query("VH_PRODA")
    cfg = pipe.global_cfg()
    cfg["sources"]["FAB"]["time_col"] = "no_such_col"
    pipe.save_global_cfg(cfg)

    c = _check(_stage(diagnose(pipe, "VH_PRODA"), "raw"), "FAB 파티션 기준 열")
    assert c["status"] == "fail" and "no_such_col" in c["detail"]
