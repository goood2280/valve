"""feature_pipeline — Ref 3단계(raw→event→feature) + 리포트/알람 순환 검증."""
import shutil
from pathlib import Path

import pytest
import yaml

from backend.core.alert_store import AlertStore
from backend.core.csv_sync import CsvSync
from backend.core.feature_pipeline import FeaturePipeline
from backend.core.pipeline_runner import PipelineRunner
from backend.core.runtime_env import plan_workers
from backend.core.s3_up import S3Uploader

REPO = Path(__file__).parent.parent


@pytest.fixture()
def pipe(tmp_path):
    shutil.copytree(REPO / "config", tmp_path / "config")
    # knob 룰북은 기준본으로 고정 — 운영 중 flow 판정이 repo config 에 반영되면
    # (csv_sync) 데모 미매핑 ppid 가 전부 매핑되어 knob-miss/알람 테스트가 흔들린다.
    (tmp_path / "config" / "feature_rules" / "ppid_knob.csv").write_text(
        "feature_name,function_step,rule_order,operator,value,category\n"
        "GATE_ETCH,GATE_ETCH,R1,eq,PP_GE_A1,KNOB_A\n"
        "GATE_ETCH,GATE_ETCH,R2,eq,PP_GE_A2,KNOB_B\n"
        "GATE_ETCH,GATE_ETCH,R3,eq,PP_X9_2300,KNOB_NEW\n"
        "GATE_ETCH,GATE_ETCH,RO,,,\n"
        "10.0 CONTACT,CONTACT_ETCH,R1,eq,PP_CE_B1,KNOB_STD\n"
        "10.0 CONTACT,CONTACT_ETCH,RO,,,\n"
        "METAL_ETCH,METAL_ETCH,R1,eq,PP_ME_C1,KNOB_M1\n"
        "METAL_ETCH,METAL_ETCH,RO,,,\n",
        encoding="utf-8")
    return FeaturePipeline(tmp_path, {})


@pytest.fixture()
def fake_s3(tmp_path):
    return S3Uploader({"s3": {"bucket": "flow-datalake",
                              "fake_local_path": str(tmp_path / "s3_local")}})


def test_raw_query_extracts_configured_sources(pipe):
    pipe.run_raw_query("VH_PRODA")
    # raw 는 소스 > vehicle > date=hive 구조 (FAB/VH_PRODA/date=…)
    raw_root = pipe.db_root() / "1.RAWDATA_DB"
    assert {d.name for d in raw_root.iterdir()} == {"FAB", "INLINE", "VM", "ET"}
    for src in ("FAB", "INLINE", "VM", "ET"):
        assert {d.name for d in (raw_root / src).iterdir()} == {"VH_PRODA"}


def test_et_raw_recognizes_reformatter(pipe):
    """ET raw 는 reformatter 의 CATEGORY=REAL ITEMID 만 저장 (auto report 동일)."""
    import polars as pl
    stats = pipe.run_raw_query("VH_PRODA")
    assert stats["rows"]["ET"] > 0
    ref = stats["reformatter"]["ET"]
    assert ref["found"] and ref["items"] == 5

    raw = pl.read_parquet(next(pipe.raw_dir("VH_PRODA", "ET").glob("date=*/part-000.parquet")))
    assert set(raw["item_id"].unique().to_list()) == {
        "ET_VTH_N", "ET_VTH_P", "ET_IDSAT_N", "ET_IDSAT_P", "ET_PCHK_CONT"}
    assert "et_value" in raw.columns
    # ADDP 파생 alias 는 raw 에 없음
    assert not any(a in raw["item_id"].to_list() for a in ("VTH_AVG", "VTH_DIFF"))


def test_et_reformatter_is_per_vehicle(pipe):
    """vehicle 별 reformatter 를 각각 인식 — PRODB 는 자기 파일의 REAL 항목만."""
    import polars as pl
    pipe.run_raw_query("VH_PRODB")
    raw = pl.read_parquet(next(pipe.raw_dir("VH_PRODB", "ET").glob("date=*/part-000.parquet")))
    assert set(raw["item_id"].unique().to_list()) == {"ET_VTH_N", "ET_IDSAT_N", "ET_PCHK_LKG"}


def test_et_skips_vehicle_without_reformatter(pipe):
    """reformatter 파일이 없는 vehicle 은 ET raw 를 만들지 않고, 다른 소스는 정상."""
    (pipe.root / "config" / "reformatter" / "VH_PRODB_reformatter.csv").unlink()
    stats = pipe.run_raw_query("VH_PRODB")
    assert stats["rows"]["ET"] == 0
    assert stats["reformatter"]["ET"]["found"] is False
    assert not list(pipe.raw_dir("VH_PRODB", "ET").glob("date=*"))
    assert stats["rows"]["FAB"] > 0 and stats["rows"]["INLINE"] > 0


def test_et_reformatter_edit_reflected_next_run(pipe):
    """reformatter 수정(REAL 항목 축소) → 재시작 없이 다음 raw 부터 반영 (fresh 로드)."""
    import polars as pl
    fp = pipe.root / "config" / "reformatter" / "VH_PRODA_reformatter.csv"
    lines = fp.read_text(encoding="utf-8").splitlines()
    fp.write_text("\n".join(lines[:3]) + "\n", encoding="utf-8")  # 헤더 + REAL 2건만

    pipe.run_raw_query("VH_PRODA")
    raw = pl.read_parquet(next(pipe.raw_dir("VH_PRODA", "ET").glob("date=*/part-000.parquet")))
    assert set(raw["item_id"].unique().to_list()) == {"ET_VTH_N", "ET_VTH_P"}


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
    raw = pl.read_parquet(next(pipe.raw_dir("VH_PRODA", "FAB").glob("date=*/part-000.parquet")))
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

    # 미확인예정 처리 → 활성에서 빠지지만, 발행에는 status 를 달고 계속 포함
    # (flow 화면에서 계속 보이고 나중에 되돌릴 수 있어야 함)
    target = next(a for a in listed["alerts"] if a["type"] == "unmatched_step")
    store.set_ack(target["id"], "미확인예정", note="flow 확인 대기")
    listed2 = store.list_alerts()
    assert listed2["suppressed"] == 1
    assert next(a for a in listed2["alerts"] if a["id"] == target["id"])["status"] == "미확인예정"

    assert store.publish("VH_PRODA")
    import json
    published = json.loads(fake_s3.get_text("valve-alerts/pipeline/VH_PRODA.json"))
    pub_target = next(a for a in published["alerts"] if a["id"] == target["id"])
    assert pub_target["status"] == "미확인예정"
    assert published["count"] == len(published["alerts"]) - 1  # count 는 활성 건만
    assert published["suppressed"] == 1

    # 다시 active 로 되돌리면 재노출
    store.set_ack(target["id"], "active")
    assert store.list_alerts()["suppressed"] == 0


def test_worker_plan_from_env_and_override():
    # auto — 코어 기반, 최소 1 이상. raw 는 API 상한(기본 3) 에 종속
    auto = plan_workers({})
    assert auto.raw_workers >= 1 and auto.vehicle_workers >= 1
    assert auto.cpu_cores >= 1 and auto.sizing == "auto"
    assert auto.raw_workers <= 3          # 기본 raw_api_max
    # 16코어 여유메모리 모사(mem_per 작게) → raw 는 3 으로 묶이고 event/feature 는 더 씀
    big = plan_workers({"cpu_cores": 16, "mem_per_worker_gb": 1})
    assert big.raw_workers == 3 and big.raw_api_max == 3
    assert big.vehicle_workers > big.raw_workers      # compute 기반, raw 상한과 분리
    assert big.feature_workers > big.raw_workers
    # raw_api_max 조정 시 raw 동시 상한만 바뀜
    loose = plan_workers({"cpu_cores": 16, "raw_api_max": 6, "mem_per_worker_gb": 1})
    assert loose.raw_workers == 6
    # 수동 max_workers override 도 raw 상한 적용
    manual = plan_workers({"max_workers": 8})
    assert manual.raw_workers == 3 and manual.sizing == "config"


def test_runtime_days_override_controls_split(pipe):
    # runtime.raw_days=5, split_days=1 → 5일 + 오늘 = 6 파티션(1일 단위)
    cfg = pipe.global_cfg()
    cfg["runtime"] = {"raw_days": 5, "split_days": 1}
    pipe.save_global_cfg(cfg)
    units = pipe._raw_units(pipe.vehicle_cfg("VH_PRODA"))
    # 소스 3종 × 6일 = 18 유닛, 날짜 6종
    dates = {u[1] for u in units}
    assert len(dates) == 6
    assert len(units) == 6 * len(pipe.sources_cfg())


def test_runner_parallel_run_all_matches_sequential(pipe):
    runner = PipelineRunner(pipe)
    plan = plan_workers({"max_workers": 4, "vehicle_workers": 2})
    summary = runner.run_all(plan)
    assert summary["ok"]
    assert set(summary["vehicles"]) == {"VH_PRODA", "VH_PRODB"}
    for v, r in summary["vehicles"].items():
        assert r["raw_rows"]["FAB"] > 0
        assert all(er > 0 for er in r["event"].values())     # 3소스 event 산출
        assert sum(r["feature"].values()) > 0                # feature 산출
        assert not r["errors"]


def test_new_source_extends_via_config(pipe):
    """ET 같은 신규 소스를 pipeline.yaml 확장만으로 raw+event 처리 (코드 수정 없이)."""
    import polars as pl
    cfg = pipe.global_cfg()
    cfg["sources"]["ET"] = {
        "table": "RAW_ET_DATA",
        "columns": ["root_lot_id", "wafer_id", "test_item", "value", "time"],
        "match": {"kind": "item", "rules": "et", "id_col": "test_item"},
    }
    cfg["feature_rules"]["et"] = "config/feature_rules/et.csv"
    pipe.save_global_cfg(cfg)
    (pipe.root / "config/feature_rules/et.csv").write_text(
        "test_item,agg\nET_01,mean\nET_02,mean\n", encoding="utf-8")

    pipe.run_raw_query("VH_PRODA")
    # 신규 소스가 raw 로 생성됨 (SOURCE/vehicle/date 구조)
    et_raw = list(pipe.raw_dir("VH_PRODA", "ET").glob("date=*/part-000.parquet"))
    assert et_raw
    cols = pl.read_parquet(et_raw[0]).columns
    assert "test_item" in cols

    r = pipe.run_event("VH_PRODA")
    assert "ET" in r
    ev = pipe._load_event("VH_PRODA", "ET")
    assert ev is not None and set(ev["test_item"].unique()) <= {"ET_01", "ET_02"}


def test_publish_saves_snapshot_meta_with_delta(pipe, fake_s3):
    pipe.run_all("VH_PRODA")
    store = AlertStore(pipe, fake_s3, {"alerts": {"s3_prefix": "valve-alerts"}}, pipe.root)

    # 최초 발행 → 전부 new, first_seen 기록, 메타 파일 저장
    p1 = store.publish("VH_PRODA")
    assert p1 and p1["count"] > 0
    assert set(p1["delta"]["new"]) == {a["id"] for a in p1["alerts"]}
    assert p1["delta"]["resolved"] == []
    assert all(a["first_seen_ts"] for a in p1["alerts"])
    assert store.load_pub_meta("VH_PRODA")["count"] == p1["count"]

    # 재발행(변화 없음) → new/resolved 없음, first_seen 계승
    p2 = store.publish("VH_PRODA")
    assert p2["delta"]["new"] == [] and p2["delta"]["resolved"] == []
    fs1 = {a["id"]: a["first_seen_ts"] for a in p1["alerts"]}
    assert all(a["first_seen_ts"] == fs1[a["id"]] for a in p2["alerts"])

    # 한 건 ack 억제 → 활성 기준 resolved 로 잡히지만, 발행에는 status 로 남음
    tgt = p2["alerts"][0]["id"]
    store.set_ack(tgt, "반영불필요")
    p3 = store.publish("VH_PRODA")
    assert tgt in p3["delta"]["resolved"]
    assert next(a for a in p3["alerts"] if a["id"] == tgt)["status"] == "반영불필요"
    assert p3["count"] == p2["count"] - 1


def test_event_config_version_change_rebuilds_all(pipe):
    """매칭 파일이 그대로여도 event 설정(event_lot_startwith)이 바뀌면
    전 소스 stale → raw 전체 재스캔으로 event DB 재생성."""
    pipe.run_raw_query("VH_PRODA")
    pipe.run_event("VH_PRODA")
    st = pipe.status("VH_PRODA")
    assert not any(st["event"][s]["stale"] for s in ("FAB", "INLINE", "VM"))

    vf = pipe.root / "config" / "vehicles.yaml"
    cfg = yaml.safe_load(vf.read_text(encoding="utf-8"))
    cfg["VH_PRODA"]["event_lot_startwith"] = "ZZZ"
    vf.write_text(yaml.safe_dump(cfg, allow_unicode=True), encoding="utf-8")

    st = pipe.status("VH_PRODA")
    assert all(st["event"][s]["stale"] for s in ("FAB", "INLINE", "VM"))
    r = pipe.run_event("VH_PRODA")
    assert all(v["rebuilt"] for v in r.values())
    # prefix ZZZ 는 어떤 lot 도 매칭 안 됨 → 전체 재스캔되어 event 0행
    assert all(v["event_rows"] == 0 for v in r.values())
    assert not any(pipe.status("VH_PRODA")["event"][s]["stale"] for s in ("FAB", "INLINE", "VM"))


def test_legacy_meta_without_version_is_stale(pipe):
    """구 포맷 _meta.json(sha 만 기록) 은 ver 부재 → stale 로 잡혀 1회 전체 재생성."""
    import json
    pipe.run_raw_query("VH_PRODA")
    pipe.run_event("VH_PRODA")
    meta_path = pipe.event_dir("VH_PRODA", "FAB") / "_meta.json"
    meta_path.write_text(json.dumps({"sha": pipe.matching_sha("FAB"), "ts": 0.0,
                                     "file": "config/step_matching/vehicle_matching.csv"}),
                         encoding="utf-8")
    assert pipe.status("VH_PRODA")["event"]["FAB"]["stale"]
    assert pipe.run_event("VH_PRODA")["FAB"]["rebuilt"]


def test_wide_form_merges_vehicle_features(pipe):
    import polars as pl
    r = pipe.run_all("VH_PRODA")
    assert r["wide"]["rows"] > 0 and r["wide"]["features"] > 0

    wide = pl.read_parquet(pipe.wide_dir() / "ML_TABLE_VH_PRODA.parquet")
    # KEY 3열이 맨 앞, PRODUCT 는 vehicles.yaml 의 product
    assert wide.columns[:3] == ["PRODUCT", "ROOT_LOT_ID", "WAFER_ID"]
    assert wide["PRODUCT"].unique().to_list() == ["PRODA"]
    # wafer 단위 1행 (KEY 중복 없음)
    assert wide.height == wide.unique(subset=["ROOT_LOT_ID", "WAFER_ID"]).height
    # 카테고리 컬럼이 병합됨 + 그룹 순서 (KNOB 이 FAB 보다 앞)
    cols = wide.columns
    for p in ("KNOB_", "FAB_", "MASK_", "INLINE_", "VM_"):
        assert any(c.startswith(p) for c in cols), f"{p} 컬럼 없음"
    assert min(i for i, c in enumerate(cols) if c.startswith("KNOB_")) \
        < min(i for i, c in enumerate(cols) if c.startswith("FAB_"))


def test_send_form_groups_split_with_mask_in_fab(pipe):
    import polars as pl
    pipe.run_all("VH_PRODA")
    pipe.run_all("VH_PRODB")
    r = pipe.run_send_form()
    assert set(r["tables"]) == {"ML_TABLE_VH_PRODA.parquet", "ML_TABLE_VH_PRODB.parquet"}

    # FAB 그룹에는 FAB_ + MASK_ 만, KNOB 그룹에는 KNOB_ 만
    fab = pl.read_parquet(pipe.send_dir() / "1.FAB" / "FAB_ML_TABLE.parquet")
    assert any(c.startswith("FAB_") for c in fab.columns)
    assert any(c.startswith("MASK_") for c in fab.columns)
    assert not any(c.startswith(("KNOB_", "INLINE_", "VM_")) for c in fab.columns)
    knob = pl.read_parquet(pipe.send_dir() / "0.KNOB" / "KNOB_ML_TABLE.parquet")
    assert any(c.startswith("KNOB_") for c in knob.columns)
    assert not any(c.startswith("MASK_") for c in knob.columns)

    # 두 vehicle 의 행이 합쳐짐 + csv 도 생성
    assert set(fab["PRODUCT"].unique().to_list()) == {"PRODA", "PRODB"}
    for g, fname in (("0.KNOB", "KNOB"), ("1.FAB", "FAB"), ("2.VM", "VM"), ("3.INLINE", "INLINE")):
        assert (pipe.send_dir() / g / f"{fname}_ML_TABLE.csv").exists()


def test_custom_feature_funcs_from_config_file(pipe):
    """config/feature_funcs.py 에 함수를 추가하면 fab.csv 의 feature_name/agg 로 즉시 사용."""
    import polars as pl
    import re
    # 관리자가 새 값 생성 함수 추가 (Ref 예시 ecuall/agg_valid_eqp 는 템플릿에 이미 존재)
    funcs = pipe.root / "config" / "feature_funcs.py"
    funcs.write_text(funcs.read_text(encoding="utf-8") + (
        "\n\ndef my_model():\n    return pl.col('eqp_model').cast(pl.Utf8)\n"),
        encoding="utf-8")
    fab = pipe.root / "config" / "feature_rules" / "fab.csv"
    fab.write_text(fab.read_text(encoding="utf-8")
                   + "GATE_ETCH,my_model,last\nGATE_ETCH,ecuall,valid_eqp\n", encoding="utf-8")

    pipe.run_raw_query("VH_PRODA")
    pipe.run_event("VH_PRODA")
    r = pipe.run_feature("VH_PRODA")
    assert "FAB_GATE_ETCH_my_model.parquet" in r["files"]["fab"]
    df = pl.read_parquet(pipe.feature_dir("VH_PRODA") / "FAB_GATE_ETCH_my_model.parquet")
    assert set(df["FAB_GATE_ETCH_my_model"].drop_nulls().to_list()) <= {"E-3000"}
    # valid_eqp (Ref 동일 — '_뒤 숫자' 있는 유효값만): ecuall 결과가 전부 패턴 충족
    ecu = pl.read_parquet(pipe.feature_dir("VH_PRODA") / "FAB_GATE_ETCH_ecuall.parquet")
    vals = ecu["FAB_GATE_ETCH_ecuall"].drop_nulls().to_list()
    assert vals and all(re.search(r"_[A-Za-z0-9]*[0-9]", v) for v in vals)


def test_unknown_feature_name_skipped_with_reason(pipe):
    fab = pipe.root / "config" / "feature_rules" / "fab.csv"
    fab.write_text(fab.read_text(encoding="utf-8") + "GATE_ETCH,no_such_func,last\n",
                   encoding="utf-8")
    pipe.run_raw_query("VH_PRODA")
    pipe.run_event("VH_PRODA")
    r = pipe.run_feature("VH_PRODA")
    hit = [s for s in r["skipped"] if s["feature"] == "FAB_GATE_ETCH_no_such_func"]
    assert hit and "feature_funcs.py" in hit[0]["reason"]  # 추가 방법 안내 포함


def test_knob_agg_adjustable_per_step(pipe):
    """knob 은 agg 컬럼이 없으면 기본 last — 있으면 step 별 조정
    (내장 first/last/valid_eqp/… 또는 feature_funcs.py 의 임의 agg_<이름>)."""
    import polars as pl
    pipe.run_raw_query("VH_PRODA")
    pipe.run_event("VH_PRODA")

    # agg 열 없음(기존 csv 그대로) → 기본 last 로 동작
    r0 = pipe.run_feature("VH_PRODA")
    assert "KNOB_GATE_ETCH_ppid.parquet" in r0["files"]["knob"]

    # 관리자 임의 함수: 특정 knob(NEW 계열)만 선택하는 집계를 feature_funcs.py 에 추가
    funcs = pipe.root / "config" / "feature_funcs.py"
    funcs.write_text(funcs.read_text(encoding="utf-8") + (
        "\n\ndef agg_pick_new():\n"
        "    v = pl.col('val').cast(pl.Utf8)\n"
        "    return v.filter(v.str.contains('NEW')).first()\n"), encoding="utf-8")

    # GATE_ETCH 만 pick_new 로 조정, 나머지는 agg 빈칸(기본 last)
    # — 룰 형식(ppid_knob.csv) 에서도 agg 컬럼이 knob_map 을 통해 전달되는지 검증
    knob = pipe.root / pipe.global_cfg()["feature_rules"]["knob"]
    rows = knob.read_text(encoding="utf-8").strip().splitlines()
    out = [rows[0] + ",agg"]
    for line in rows[1:]:
        out.append(line + (",pick_new" if ",GATE_ETCH," in line else ","))
    knob.write_text("\n".join(out) + "\n", encoding="utf-8")

    r1 = pipe.run_feature("VH_PRODA")
    assert "KNOB_GATE_ETCH_ppid.parquet" in r1["files"]["knob"]
    ge = pl.read_parquet(pipe.feature_dir("VH_PRODA") / "KNOB_GATE_ETCH_ppid.parquet")
    vals = ge["KNOB_GATE_ETCH_ppid"].drop_nulls().to_list()
    # 임의 선택 함수 적용됨 (step 미통과 wafer 는 knob skip 판정의 SKIP)
    assert vals and set(vals) - {"SKIP"} == {"KNOB_NEW"}
    # agg 미지정 step 은 기본 last 유지 (knob/RO 문자열)
    ce = pl.read_parquet(pipe.feature_dir("VH_PRODA") / "KNOB_CONTACT_ETCH_ppid.parquet")
    cvals = set(ce["KNOB_CONTACT_ETCH_ppid"].drop_nulls().to_list())
    assert any(v.startswith(("KNOB_", "PP_")) for v in cvals)


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


def test_knob_skip_auto_marks_passed_wafers(pipe):
    """명시 SKIP 블록이 없어도, 뒤쪽 step(공동 통과 wafer 의 tkout_time 상대순서로
    판별)을 이미 지난 빈 wafer 는 auto 판정으로 "SKIP". route 마지막 knob step 은
    뒤쪽 anchor 가 없어 보류(null 유지 + 리포트) — 과잉 skip 방지."""
    import polars as pl
    pipe.run_raw_query("VH_PRODA")
    pipe.run_event("VH_PRODA")
    r = pipe.run_feature("VH_PRODA")
    skips = [s for s in r["knob_skip"] if s["mode"] == "auto"]
    assert skips and all(s["vehicle"] == "VH_PRODA" for s in skips)
    assert any(s["feature"] == "KNOB_GATE_ETCH_ppid" for s in skips)
    ge = pl.read_parquet(pipe.feature_dir("VH_PRODA") / "KNOB_GATE_ETCH_ppid.parquet")
    assert "SKIP" in set(ge["KNOB_GATE_ETCH_ppid"].to_list())
    # CONTACT_ETCH 는 매칭된 route 의 마지막 step — anchor 없음 → skip 하지 않음
    ce = pl.read_parquet(pipe.feature_dir("VH_PRODA") / "KNOB_CONTACT_ETCH_ppid.parquet")
    assert "SKIP" not in set(ce["KNOB_CONTACT_ETCH_ppid"].to_list())
    assert any(s["feature"] == "KNOB_CONTACT_ETCH_ppid" and "보류" in s["reason"]
               for s in r["skipped"])
    # 리포트 파일로도 남음
    assert pipe.load_report("VH_PRODA", "knob_skip") == r["knob_skip"]


def test_knob_skip_rule_block_next_main_step(pipe):
    """사내 형식: 같은 feature+rule_order 복수 행 = AND 블록.
    "knob step _null AND 다음 main step not_null → SKIP" 이 명시 판정으로 동작,
    블록이 있는 feature 는 auto 가 덮지 않는다."""
    import polars as pl
    knob = pipe.root / pipe.global_cfg()["feature_rules"]["knob"]
    knob.write_text(knob.read_text(encoding="utf-8")
                    + "GATE_ETCH,GATE_ETCH,R9,_null,,SKIP\n"
                      "GATE_ETCH,SPACER_CVD,R9,not_null,,SKIP\n",
                    encoding="utf-8")
    pipe.run_raw_query("VH_PRODA")
    pipe.run_event("VH_PRODA")
    r = pipe.run_feature("VH_PRODA")
    ge_skips = [s for s in r["knob_skip"] if s["feature"] == "KNOB_GATE_ETCH_ppid"]
    assert ge_skips and all(s["mode"] == "rule" for s in ge_skips)
    ge = pl.read_parquet(pipe.feature_dir("VH_PRODA") / "KNOB_GATE_ETCH_ppid.parquet")
    assert "SKIP" in set(ge["KNOB_GATE_ETCH_ppid"].to_list())
    # AND 블록 행이 per-step eq 매핑으로 새지 않음 (조건 step 에 SKIP 매핑 오염 금지)
    vmap = pipe.knob_map("VH_PRODA")
    assert vmap.filter(vmap["knob"] == "SKIP").height == 0


def test_knob_skip_block_unresolved_step_guarded(pipe):
    """SKIP 블록의 조건 step 이 이 vehicle 매칭에 없으면 skip 을 적용하지 않고
    리포트 — step 매칭 오류/오타가 조용히 skip 으로 둔갑하지 않게."""
    knob = pipe.root / pipe.global_cfg()["feature_rules"]["knob"]
    knob.write_text(knob.read_text(encoding="utf-8")
                    + "GATE_ETCH,GATE_ETCH,R9,_null,,SKIP\n"
                      "GATE_ETCH,NO_SUCH_STEP,R9,not_null,,SKIP\n",
                    encoding="utf-8")
    pipe.run_raw_query("VH_PRODA")
    pipe.run_event("VH_PRODA")
    r = pipe.run_feature("VH_PRODA")
    # 블록 미적용 + auto 도 개입 안 함 (명시 블록이 있는 feature 는 사용자 정의 우선)
    assert not [s for s in r["knob_skip"] if s["feature"] == "KNOB_GATE_ETCH_ppid"]
    assert any("NO_SUCH_STEP" in s["reason"] for s in r["skipped"])
