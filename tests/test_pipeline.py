"""feature_pipeline — Ref 3단계(raw→event→feature) + 리포트/알람 순환 검증."""
import shutil
from pathlib import Path

import pytest
import yaml

from backend.core.alert_store import AlertStore
from backend.core.csv_sync import CsvSync
from backend.core.feature_pipeline import FeaturePipeline
from backend.core.s3_up import S3Uploader

REPO = Path(__file__).parent.parent


@pytest.fixture()
def pipe(tmp_path):
    shutil.copytree(REPO / "config", tmp_path / "config")
    return FeaturePipeline(tmp_path, {})


@pytest.fixture()
def fake_s3(tmp_path):
    return S3Uploader({"s3": {"bucket": "flow-datalake",
                              "fake_local_path": str(tmp_path / "s3_local")}})


def test_raw_query_extracts_only_three_sources(pipe):
    pipe.run_raw_query("VH_PRODA")
    # raw 는 소스 > 제품 > date=hive 구조
    raw_root = pipe.db_root() / "1.RAWDATA_DB"
    assert {d.name for d in raw_root.iterdir()} == {"FAB", "INLINE", "VM"}
    for src in ("FAB", "INLINE", "VM"):
        assert {d.name for d in (raw_root / src).iterdir()} == {"PRODA"}


def test_source_columns_config_is_applied(pipe):
    import polars as pl
    # FAB 컬럼에서 ppid 제거 → raw 에서 빠지고, KNOB feature 는 사유와 함께 skip
    cfg = pipe.global_cfg()
    fab_cols = [c for c in cfg["sources"]["FAB"]["columns"] if c != "ppid"]
    cfg["sources"]["FAB"]["columns"] = fab_cols
    cfg["sources"]["FAB"]["table"] = "MY_FAB_TABLE"
    pipe.save_global_cfg(cfg)

    stats = pipe.run_raw_query("VH_PRODA")
    assert stats["tables"]["FAB"] == "MY_FAB_TABLE"
    raw = pl.read_parquet(next(pipe.raw_dir("PRODA", "FAB").glob("date=*/part-000.parquet")))
    assert "ppid" not in raw.columns
    assert set(fab_cols) <= set(raw.columns)

    pipe.run_event("VH_PRODA")
    r = pipe.run_feature("VH_PRODA")
    assert r["features"]["knob"] == 0
    assert any(s["feature"] == "KNOB_*" for s in r["skipped"])
    # 나머지 카테고리는 정상 산출
    assert r["features"]["fab"] > 0 and r["features"]["inline"] > 0


def test_full_run_produces_all_categories(pipe):
    r = pipe.run_all("VH_PRODA")
    # raw: 3 소스 모두 생성
    assert r["raw"]["rows"]["FAB"] > 0
    assert r["raw"]["rows"]["INLINE"] > 0
    assert r["raw"]["rows"]["VM"] > 0
    # event: 3 소스 모두 매칭 필터로 반드시 줄어듦
    #  FAB/VM — vehicle_matching step 필터, INLINE — inline matching item 필터
    for src in ("FAB", "INLINE", "VM"):
        e = r["event"][src]
        assert 0 < e["event_rows"] < e["raw_rows"], f"{src} event 필터 미동작"
    # feature: 5개 카테고리 전부 산출
    for cat in ("fab", "knob", "mask", "inline", "vm"):
        assert r["feature"]["features"][cat] > 0, f"{cat} feature 없음"
    # 파일 prefix 가 카테고리와 일치
    listed = pipe.list_features("VH_PRODA")
    assert all(f["file"].startswith("FAB_") for f in listed["fab"])
    assert all(f["file"].startswith("KNOB_") for f in listed["knob"])


def test_unmatched_scan_respects_global_exclude(pipe):
    pipe.run_raw_query("VH_PRODA")
    rep = pipe.scan_unmatched("VH_PRODA")
    shown = {x["step_id"] for x in rep["unmatched"]}
    excluded = {x["step_id"]: x["excluded_by"] for x in rep["excluded"]}
    assert "XX777700" in shown                       # 진짜 미매칭 → 노출
    assert "AX550000" in excluded                    # eqp_id AUX_* 제외
    assert "eqp_id" in excluded["AX550000"]
    assert "MT100200" in excluded                    # eqp_model MEA-* 제외
    assert "eqp_model" in excluded["MT100200"]
    # 매칭된 step 은 아예 안 나옴
    assert "CC942300" not in shown | set(excluded)


def test_exclude_config_edit_changes_scan(pipe):
    pipe.run_raw_query("VH_PRODA")
    cfg = pipe.global_cfg()
    cfg["unmatched_scan"]["exclude"] = {"eqp_id": [], "eqp_model": []}
    pipe.save_global_cfg(cfg)
    rep = pipe.scan_unmatched("VH_PRODA")
    shown = {x["step_id"] for x in rep["unmatched"]}
    assert {"XX777700", "AX550000", "MT100200"} <= shown
    assert rep["excluded"] == []


def test_knob_miss_reports_vehicle_and_split(pipe):
    r = pipe.run_all("VH_PRODA")
    miss = r["feature"]["knob_miss"]
    assert miss, "knob 미변환(RO) 건이 있어야 함"
    splits = set(r["raw"]["splits"])
    for m in miss:
        assert m["vehicle"] == "VH_PRODA"
        assert m["split"] in splits
        assert m["ppid"].startswith("PP_X9_")        # 매핑에 없는 raw ppid
        assert m["n_lots"] >= 1 and m["lots"]
    # 리포트 파일로도 남음
    assert pipe.load_report("VH_PRODA", "knob_miss") == miss


def test_status_tracks_event_progress_and_stale_matching(pipe):
    # raw 만 실행 → 전 소스 event 미처리(pending)
    pipe.run_raw_query("VH_PRODA")
    st = pipe.status("VH_PRODA")
    for src in ("FAB", "INLINE", "VM"):
        assert st["event"][src]["dates"] == []
        assert st["event"][src]["pending"] == st["raw"][src]

    # event 처리 후 → 전 소스 완료 + 적용 버전(applied_ts/sha) 기록
    pipe.run_event("VH_PRODA")
    st = pipe.status("VH_PRODA")
    for src in ("FAB", "INLINE", "VM"):
        e = st["event"][src]
        assert e["pending"] == [] and not e["stale"]
        assert e["dates"] == st["raw"][src]
        assert e["applied_ts"] and e["matching_sha"]

    # vehicle_matching 내용 변경 → FAB·VM 만 stale (INLINE 은 inline matching 기준)
    matching = pipe.root / pipe.global_cfg()["step_matching"]
    matching.write_text(matching.read_text(encoding="utf-8")
                        + "VH_PRODA,ZZ999900,NEW_STEP\n", encoding="utf-8")
    st = pipe.status("VH_PRODA")
    assert st["event"]["FAB"]["stale"] and st["event"]["VM"]["stale"]
    assert not st["event"]["INLINE"]["stale"]

    # 재실행 → 해당 소스만 전체 재생성(rebuilt), stale 해소
    r = pipe.run_event("VH_PRODA")
    assert r["FAB"]["rebuilt"] and r["VM"]["rebuilt"] and not r["INLINE"]["rebuilt"]
    st = pipe.status("VH_PRODA")
    assert not any(st["event"][s]["stale"] for s in ("FAB", "INLINE", "VM"))


def test_inline_matching_change_rebuilds_inline_event(pipe):
    import polars as pl
    pipe.run_raw_query("VH_PRODA")
    pipe.run_event("VH_PRODA")
    before = pl.concat([pl.read_parquet(f) for f in
                        pipe.event_dir("VH_PRODA", "INLINE").glob("date=*/part-000.parquet")])
    assert set(before["item_id"].unique()) == {"ITEM_CD_001", "ITEM_THK_002"}

    # inline matching 에 item 추가 → INLINE 만 stale → 전체 재생성 후 item 반영
    inline = pipe.root / "config/feature_rules/inline.csv"
    inline.write_text(inline.read_text(encoding="utf-8") + "ITEM_OVL_003,mean\n", encoding="utf-8")
    st = pipe.status("VH_PRODA")
    assert st["event"]["INLINE"]["stale"] and not st["event"]["FAB"]["stale"]

    r = pipe.run_event("VH_PRODA")
    assert r["INLINE"]["rebuilt"]
    after = pl.concat([pl.read_parquet(f) for f in
                       pipe.event_dir("VH_PRODA", "INLINE").glob("date=*/part-000.parquet")])
    assert "ITEM_OVL_003" in set(after["item_id"].unique())


def test_csv_sync_pulls_from_s3(pipe, fake_s3):
    sync = CsvSync(pipe.root, fake_s3)
    updated_dests = []
    sync.on_updated = updated_dests.extend  # 갱신 훅 (router 가 event 재생성에 사용)
    sync.save_config({
        "enabled": True, "interval_min": 5, "s3_prefix": "flow/artifacts",
        "files": [{"key": "matching/step_matching.csv",
                   "dest": "config/step_matching/vehicle_matching.csv"}],
    })
    # flow 가 올린 파일 모사
    csv_text = "vehicle,step_id,step_desc\nVH_PRODA,CC942300,GATE_ETCH\n"
    fake_s3.put_text("flow/artifacts/matching/step_matching.csv", csv_text)

    r1 = sync.sync_now()
    assert r1[0]["status"] == "updated"
    assert updated_dests == ["config/step_matching/vehicle_matching.csv"]
    assert (pipe.root / "config/step_matching/vehicle_matching.csv").read_text(encoding="utf-8") == csv_text
    # 내용 동일하면 쓰기 생략
    assert sync.sync_now()[0]["status"] == "unchanged"
    # S3 에 없는 key 는 missing
    sync.save_config({"enabled": True, "interval_min": 5, "s3_prefix": "flow/artifacts",
                      "files": [{"key": "matching/none.csv", "dest": "config/none.csv"}]})
    assert sync.sync_now()[0]["status"] == "missing"


def test_alert_store_ack_suppresses_realert(pipe, fake_s3):
    pipe.run_all("VH_PRODA")
    store = AlertStore(pipe, fake_s3, {"alerts": {"s3_prefix": "valve-alerts"}}, pipe.root)

    listed = store.list_alerts()
    types = {a["type"] for a in listed["alerts"]}
    assert {"unmatched_step", "ro_ppid"} <= types
    assert listed["active"] == len(listed["alerts"])

    # 미확인예정 처리 → 활성에서 빠지고, 발행(publish)에서도 제외
    target = next(a for a in listed["alerts"] if a["type"] == "unmatched_step")
    store.set_ack(target["id"], "미확인예정", note="flow 확인 대기")
    listed2 = store.list_alerts()
    assert listed2["suppressed"] == 1
    assert next(a for a in listed2["alerts"] if a["id"] == target["id"])["status"] == "미확인예정"

    assert store.publish("VH_PRODA")
    import json
    published = json.loads(fake_s3.get_text("valve-alerts/pipeline/VH_PRODA.json"))
    assert all(a["id"] != target["id"] for a in published["alerts"])

    # 다시 active 로 되돌리면 재노출
    store.set_ack(target["id"], "active")
    assert store.list_alerts()["suppressed"] == 0


def test_knob_feature_keeps_raw_ppid_for_miss(pipe):
    import polars as pl
    pipe.run_raw_query("VH_PRODA")
    pipe.run_event("VH_PRODA")
    r = pipe.run_feature("VH_PRODA")
    knob_files = r["files"]["knob"]
    assert knob_files
    df = pl.concat([pl.read_parquet(pipe.feature_dir("VH_PRODA") / f) for f in knob_files],
                   how="diagonal")
    vals = set()
    for c in df.columns:
        if c.startswith("KNOB_"):
            vals |= set(df[c].drop_nulls().to_list())
    assert any(v.startswith("KNOB_") for v in vals)      # 매핑 성공분
    assert any(v.startswith("PP_X9_") for v in vals)     # 미변환분은 raw ppid 유지(RO)
