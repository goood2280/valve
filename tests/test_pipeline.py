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
    p = FeaturePipeline(tmp_path, {})
    # 실행 금지 시간대는 기본 on(00:00~02:00) — 켠 채로 두면 스케줄 테스트가
    # "새벽에 돌리면 실패" 하는 시계 의존 테스트가 된다. 필요한 테스트만 직접 켠다.
    cfg = p.global_cfg()
    cfg["runtime"]["quiet_enabled"] = False
    p.save_global_cfg(cfg)
    return p


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

    raw = pl.read_parquet(next(pipe.raw_dir("VH_PRODA", "ET").glob("date=*/data.parquet")))
    assert set(raw["item_id"].unique().to_list()) == {
        "ET_VTH_N", "ET_VTH_P", "ET_IDSAT_N", "ET_IDSAT_P", "ET_PCHK_CONT"}
    assert "et_value" in raw.columns
    # ADDP 파생 alias 는 raw 에 없음
    assert not any(a in raw["item_id"].to_list() for a in ("VTH_AVG", "VTH_DIFF"))


def test_et_reformatter_is_per_vehicle(pipe):
    """vehicle 별 reformatter 를 각각 인식 — PRODB 는 자기 파일의 REAL 항목만."""
    import polars as pl
    pipe.run_raw_query("VH_PRODB")
    raw = pl.read_parquet(next(pipe.raw_dir("VH_PRODB", "ET").glob("date=*/data.parquet")))
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
    raw = pl.read_parquet(next(pipe.raw_dir("VH_PRODA", "ET").glob("date=*/data.parquet")))
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
    raw = pl.read_parquet(next(pipe.raw_dir("VH_PRODA", "FAB").glob("date=*/data.parquet")))
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


def test_status_reports_feature_covered_span(pipe):
    """feat 배지의 근거 — feature 가 며칠치(언제~언제) event 를 담았는지.
    feature parquet 에는 날짜 컬럼이 없어서 산출 시점에 남긴 _meta.json 이 유일한 근거다."""
    pipe.run_raw_query("VH_PRODA")
    pipe.run_event("VH_PRODA")

    # feature 산출 전 — 기록이 없으니 현재 event 기준 추정으로 채운다
    st = pipe.status("VH_PRODA")
    assert st["feature_ts"] is None
    assert st["feature_cov"]["FAB"]["approx"] is True

    pipe.run_feature("VH_PRODA")
    st = pipe.status("VH_PRODA")
    for src in ("FAB", "INLINE", "VM"):
        dates = st["event"][src]["dates"]
        cov = st["feature_cov"][src]
        assert cov["days"] == len(dates)
        assert (cov["start"], cov["end"]) == (dates[0], dates[-1])
        assert "approx" not in cov            # 산출 기록이 있으면 추정이 아니다
    assert st["feature_ts"] > 0
    assert sum(st["features"].values()) > 0
    # _meta.json 이 feature 목록(*.parquet)으로 새지 않아야 한다
    assert (pipe.feature_dir("VH_PRODA") / "_meta.json").exists()
    assert all(f.suffix == ".parquet" for f in pipe.feature_dir("VH_PRODA").glob("*.parquet"))


def test_raw_query_window_is_half_open(pipe):
    """유닛의 쿼리 구간은 항상 반열림 [from, to) — 마지막 (today, today) 유닛도
    하루가 되고(폭 0 이 아님), 이웃 유닛과 겹쳐 같은 날을 두 번 쿼리하지 않는다."""
    from datetime import date as _d, timedelta as _td

    from backend.core.feature_pipeline import get_split_date_ranges, raw_query_window

    today = _d(2026, 7, 28)
    ranges = get_split_date_ranges(3, 1, today=today)
    windows = [raw_query_window(s, e) for s, e in ranges]
    assert windows[-1] == (today, today + _td(days=1))       # 오늘도 하루치
    assert all(b > a for a, b in windows)                    # 폭 0 인 구간 없음
    starts = [a for a, _ in windows]
    assert len(set(starts)) == len(starts)                   # 시작일 중복 없음
    for (_, prev_end), (nxt_start, _) in zip(windows, windows[1:]):
        assert prev_end == nxt_start                         # 빈틈도 겹침도 없다

    # 실제 저장 검증 — 파티션 기준 열(FAB·ET=tkout_time, INLINE·VM=time)의 날짜로
    # 나뉘므로, 쿼리 구간이 넓어져도 각 date= 안에는 그 날 행만 들어간다.
    import polars as pl
    pipe.run_raw_query("VH_PRODA")
    # 전 소스가 tkout_time(공정 진행 시각) 기준 — 소스가 달라도 같은 날짜 축
    for src in ("FAB", "INLINE", "VM", "ET"):
        assert pipe._time_col(src) == "tkout_time", src
    for src in ("FAB", "INLINE", "VM"):
        tcol = pipe._time_col(src)
        dirs = sorted(pipe.raw_dir("VH_PRODA", src).glob("date=*"))
        assert dirs, f"{src} raw 파티션 없음"
        assert len({d.name for d in dirs}) == len(dirs)        # 날짜 중복 없음
        for d in dirs:
            df = pl.concat([pl.read_parquet(f) for f in d.glob("*.parquet")])
            got = set(df[tcol].cast(pl.Utf8).str.slice(0, 10).to_list())
            assert got == {d.name[5:]}, f"{src} {d.name} 에 다른 날짜 행이 섞임: {got}"


def test_explicit_time_col_override_is_validated(pipe):
    """파티션 기준 열은 웹(⚙)에서 바꾼다. 오타는 조용히 넘어가지 않는다
    (기준 열을 못 찾으면 하루치가 통째로 엉뚱한 파티션으로 들어간다)."""
    import pytest as _pytest

    cfg = pipe.global_cfg()
    cfg["sources"]["INLINE"]["time_col"] = "time"     # 측정 시각 기준으로 되돌리기
    pipe.save_global_cfg(cfg)
    assert pipe._time_col("INLINE") == "time"
    assert pipe.sources_view()["INLINE"]["resolved_time_col"] == "time"

    cfg = pipe.global_cfg()
    cfg["sources"]["INLINE"]["time_col"] = "no_such_time"
    pipe.save_global_cfg(cfg)
    with _pytest.raises(ValueError, match="time_col"):
        pipe._time_col("INLINE")
    # 화면은 죽지 않고 오류를 표시한다 (설정 오류로 UI 전체가 안 뜨면 고칠 수도 없다)
    view = pipe.sources_view()["INLINE"]
    assert view["resolved_time_col"] is None and "time_col" in view["error"]


def test_legacy_config_without_tkout_time_keeps_working(pipe):
    """기존 설치의 pipeline.yaml 은 seed-only 라 INLINE/VM 에 tkout_time 이 없다.
    그때 코드 기본값(tkout_time)을 강요하면 업그레이드가 파이프라인을 멈춘다 —
    조회 컬럼에 없으면 종전처럼 time 으로 내려간다."""
    cfg = pipe.global_cfg()
    cfg["sources"]["INLINE"] = {
        "table": "RAW_INLINE_DATA",
        "columns": ["root_lot_id", "wafer_id", "item_id", "value", "measure_pos", "time"],
    }
    pipe.save_global_cfg(cfg)
    assert pipe._time_col("INLINE") == "time"
    assert pipe.sources_view()["INLINE"]["error"] == ""
    pipe.run_raw_query("VH_PRODA")     # 실행도 정상
    assert pipe.status("VH_PRODA")["raw"]["INLINE"]


def test_db_usage_flags_vehicles_over_threshold(pipe):
    pipe.run_raw_query("VH_PRODA")
    pipe.run_event("VH_PRODA")
    u = pipe.db_usage(force=True)
    assert u["vehicles"]["VH_PRODA"]["bytes"] > 0
    assert u["vehicles"]["VH_PRODA"]["parts"]["raw"] > 0
    assert u["warn_gb"] == 40 and not u["warn_vehicles"]     # 기본 임계 40GB

    # 소스(FAB/INLINE/VM/ET)별 raw·event 를 따로 본다 — 무엇부터 지울지 정하는 근거
    srcs = u["vehicles"]["VH_PRODA"]["sources"]
    assert {"FAB", "INLINE", "VM"} <= set(srcs)
    assert srcs["FAB"]["raw"] > 0 and srcs["FAB"]["event"] > 0
    assert srcs["FAB"]["time_col"] == "tkout_time"
    assert srcs["ET"]["event_enabled"] is False and srcs["ET"]["event"] == 0
    assert u["by_source"]["FAB"]["bytes"] >= srcs["FAB"]["bytes"]   # 전 제품 합
    # 제품 합계 = 소스별 raw+event + feature + reports
    parts = u["vehicles"]["VH_PRODA"]["parts"]
    assert parts["raw"] == sum(d["raw"] for d in srcs.values())
    assert parts["event"] == sum(d["event"] for d in srcs.values())

    # 임계를 아주 낮추면 경고로 잡힌다 (운영에서 40GB 초과 시 나오는 그 표시)
    cfg = pipe.global_cfg()
    cfg["runtime"]["db_warn_gb"] = 0.000001
    pipe.save_global_cfg(cfg)
    u2 = pipe.db_usage(force=True)
    assert "VH_PRODA" in u2["warn_vehicles"] and u2["vehicles"]["VH_PRODA"]["warn"]

    # 캐시 — 강제하지 않으면 다시 걷지 않는다
    assert pipe.db_usage()["cached"] is True


def test_requeried_raw_refreshes_event_partition(pipe):
    """롤링 윈도우 재조회/재시도로 raw 파티션이 다시 받아지면 해당 날짜 event 도
    다시 만들어져야 한다 — 예전엔 'event 파일이 있으면 skip' 이라 늦게 도착한
    데이터가 event 에 반영되지 않았다 (첫 스냅샷 고정)."""
    import polars as pl
    pipe.run_raw_query("VH_PRODA")
    pipe.run_event("VH_PRODA")
    st = pipe.status("VH_PRODA")
    assert st["event"]["FAB"]["pending"] == []      # 방금 만들었으니 전부 최신

    # 한 날짜의 raw 를 '늦게 도착한 데이터' 로 다시 쓰기 — GATE_ETCH 행 제거
    # (다시 쓰인 mtime 이 event 파티션보다 새것이 된다)
    date_dir = sorted(pipe.raw_dir("VH_PRODA", "FAB").glob("date=*"))[0]
    d = date_dir.name[5:]
    fp = date_dir / "data.parquet"
    raw = pl.read_parquet(fp)
    pl.DataFrame(raw.filter(pl.col("step_desc") != "GATE_ETCH")).write_parquet(fp)

    st = pipe.status("VH_PRODA")
    assert d in st["event"]["FAB"]["pending"]        # 미처리로 감지

    r = pipe.run_event("VH_PRODA")
    assert not r["FAB"]["rebuilt"]                   # 전체 재생성이 아니라 그 날짜만
    ev = pl.read_parquet(pipe.event_dir("VH_PRODA", "FAB") / date_dir.name / "data.parquet")
    assert "GATE_ETCH" not in set(ev["step_desc"].unique())   # 새 raw 가 반영됨
    assert pipe.status("VH_PRODA")["event"]["FAB"]["pending"] == []


def test_inline_matching_change_rebuilds_inline_event(pipe):
    import polars as pl
    pipe.run_raw_query("VH_PRODA")
    pipe.run_event("VH_PRODA")
    before = pl.concat([pl.read_parquet(f) for f in
                        pipe.event_dir("VH_PRODA", "INLINE").glob("date=*/data.parquet")])
    assert set(before["item_id"].unique()) == {"ITEM_CD_001", "ITEM_THK_002"}

    # inline matching 에 item 추가 → INLINE 만 stale → 전체 재생성 후 item 반영
    inline = pipe.root / "config/feature_rules/inline.csv"
    inline.write_text(inline.read_text(encoding="utf-8") + "ITEM_OVL_003,mean\n", encoding="utf-8")
    st = pipe.status("VH_PRODA")
    assert st["event"]["INLINE"]["stale"] and not st["event"]["FAB"]["stale"]

    r = pipe.run_event("VH_PRODA")
    assert r["INLINE"]["rebuilt"]
    after = pl.concat([pl.read_parquet(f) for f in
                       pipe.event_dir("VH_PRODA", "INLINE").glob("date=*/data.parquet")])
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
    et_raw = list(pipe.raw_dir("VH_PRODA", "ET").glob("date=*/data.parquet"))
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


def test_raw_hive_partition_matches_auto_report_format(pipe):
    """raw 는 auto report daily DB 와 동일한 hive partitioning —
    파티션 키 = 데이터의 시간 컬럼 날짜, 파일명 = data.parquet."""
    import polars as pl
    pipe.run_raw_query("VH_PRODA")
    checked = 0
    for source in pipe.sources_cfg():
        tc = pipe._time_col(source)
        assert tc, f"{source} 시간 컬럼 없음"
        for pdir in pipe.raw_dir("VH_PRODA", source).glob("date=*"):
            files = list(pdir.glob("*.parquet"))
            assert [f.name for f in files] == ["data.parquet"], f"{pdir} 파일명 불일치"
            df = pl.read_parquet(files[0])
            dates = set(df[tc].cast(pl.Utf8).str.slice(0, 10).unique().to_list())
            assert dates == {pdir.name[5:]}, f"{pdir} 파티션 날짜 ≠ 데이터 날짜 {dates}"
            checked += 1
    assert checked > 0


def test_et_is_raw_only_no_event_db(pipe):
    """repo 기본 설정: FAB/INLINE/VM 은 raw+event, ET 는 raw 전용(event: false).
    과거에 만들어진 ET event DB 는 run_event 가 정리한다."""
    pipe.run_raw_query("VH_PRODA")
    # 과거 잔재 모사 — ET event 파티션이 이미 있던 상황
    stale_et = pipe.event_dir("VH_PRODA", "ET") / "date=2026-01-01"
    stale_et.mkdir(parents=True)
    (stale_et / "data.parquet").write_bytes(b"")

    r = pipe.run_event("VH_PRODA")
    assert set(r) == {"FAB", "INLINE", "VM"}          # ET 는 event 대상 아님
    assert not pipe.event_dir("VH_PRODA", "ET").exists()  # 잔재 정리됨
    assert list(pipe.raw_dir("VH_PRODA", "ET").glob("date=*/data.parquet"))  # raw 는 존재
    # status 도 event 는 3소스만, raw 는 ET 포함 4소스
    st = pipe.status("VH_PRODA")
    assert set(st["event"]) == {"FAB", "INLINE", "VM"}
    assert set(st["raw"]) == {"FAB", "INLINE", "VM", "ET"}


def test_legacy_part000_files_still_readable(pipe):
    """구 파일명(part-000.parquet) 파티션도 로드 호환 —
    같은 파티션을 다시 쓰면 data.parquet 로 교체되고 구 파일은 제거된다."""
    import polars as pl
    pipe.run_raw_query("VH_PRODA")
    # 구 레이아웃 모사: 어떤 파티션 하나를 part-000 파일명으로 되돌림
    pdir = next(pipe.raw_dir("VH_PRODA", "FAB").glob("date=*"))
    (pdir / "data.parquet").rename(pdir / "part-000.parquet")

    total = pipe._load_raw("VH_PRODA", "FAB")
    assert total is not None and total.height > 0     # 구 파일명 포함 로드

    ev = pipe.run_event("VH_PRODA")                   # 구 파일명 raw 도 event 처리
    assert ev["FAB"]["event_rows"] > 0

    pipe.run_raw_query("VH_PRODA")                    # 재추출 → data.parquet 로 교체
    assert not (pdir / "part-000.parquet").exists()
    assert (pdir / "data.parquet").exists()


# ── 알람 업로드 폴더 (Valve → S3 → flow 매칭알람) ──
def _outbox_store(pipe, fake_s3, **alerts):
    cfg = {"alerts": {"s3_prefix": "valve-alerts", "outbox_dir": "s3_outbox", **alerts}}
    return AlertStore(pipe, fake_s3, cfg, pipe.root)


def test_outbox_mirrors_published_alerts_byte_for_byte(pipe, fake_s3):
    """업로드 폴더의 트리/내용이 S3 발행본과 1:1 — 이 폴더만 sync 하면 flow 가 읽는다."""
    pipe.run_all("VH_PRODA")
    store = _outbox_store(pipe, fake_s3)
    assert store.publish("VH_PRODA")

    fp = pipe.root / "s3_outbox" / "valve-alerts" / "pipeline" / "VH_PRODA.json"
    assert fp.exists()
    # write_text 의 개행 변환(Windows \r\n)에 오염되지 않아야 S3 본과 같다
    assert fp.read_bytes() == fake_s3.get_text(
        "valve-alerts/pipeline/VH_PRODA.json").encode("utf-8")
    assert store.outbox_sync_dir() == pipe.root / "s3_outbox" / "valve-alerts"

    status = store.outbox_status()
    assert status["enabled"] and status["s3_prefix"] == "valve-alerts"
    assert [f["key"] for f in status["files"]] == ["valve-alerts/pipeline/VH_PRODA.json"]


def test_outbox_written_even_when_direct_s3_disabled(pipe, fake_s3):
    """직접 S3 접근이 없는 환경 — 폴더 미러가 유일한 전송로라 항상 써야 한다."""
    pipe.run_all("VH_PRODA")
    store = _outbox_store(pipe, fake_s3, s3_enabled=False)
    assert store.publish("VH_PRODA") is False          # S3 직접 발행은 안 함
    assert (pipe.root / "s3_outbox" / "valve-alerts" / "pipeline" / "VH_PRODA.json").exists()
    assert fake_s3.get_text("valve-alerts/pipeline/VH_PRODA.json") is None


def test_outbox_missing_file_forces_republish(pipe, fake_s3):
    """지문이 같아도 폴더에 파일이 없으면 다시 발행 — 폴더를 지워도 다음 사이클에 복구."""
    pipe.run_all("VH_PRODA")
    store = _outbox_store(pipe, fake_s3)
    store.publish("VH_PRODA")
    assert store.publish_if_changed("VH_PRODA")["skipped"] is True

    fp = pipe.root / "s3_outbox" / "valve-alerts" / "pipeline" / "VH_PRODA.json"
    fp.unlink()
    assert store.publish_if_changed("VH_PRODA")["skipped"] is False
    assert fp.exists()


def test_outbox_disabled_by_empty_dir(pipe, fake_s3):
    pipe.run_all("VH_PRODA")
    store = _outbox_store(pipe, fake_s3, outbox_dir="")
    assert store.publish("VH_PRODA")
    assert store.outbox_root() is None
    assert store.outbox_status()["enabled"] is False
    assert not (pipe.root / "s3_outbox").exists()


# ── ecuall 유효 장비값 + sleuth_order 채움 ──
def _fab_event(pipe, rows):
    """FAB event DB 를 직접 만들어 feature 규칙만 검증 (raw/매칭 경로 우회)."""
    import polars as pl
    edir = pipe.event_dir("VH_PRODA", "FAB") / "date=2026-07-01"
    edir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(edir / "data.parquet")


def _run_fab_only(pipe, fab_csv: str):
    (pipe.root / "config" / "feature_rules" / "fab.csv").write_text(fab_csv, encoding="utf-8")
    for name in ("inline", "vm", "mask"):
        (pipe.root / "config" / "feature_rules" / f"{name}.csv").unlink(missing_ok=True)
    return pipe.run_feature("VH_PRODA")


def test_valid_or_last_prefers_numeric_last_segment(pipe):
    """마지막 '_' 뒤에 숫자가 있는 값을 쓰고, 그런 값이 없으면 tkout_time last."""
    import polars as pl
    base = {"step_desc": "GATE_ETCH", "step_id": "CC942300", "ppid": "P", "split": "s",
            "part_id": "X", "reticle_id": "-", "eqp_model": "M", "sleuth_order": "1"}
    _fab_event(pipe, [
        # W1 — 무효(EQP_01_CH_A) 가 마지막이지만 유효값(EQP_01_CH_2)이 있으니 그걸 쓴다
        {**base, "root_lot_id": "R1", "wafer_id": "1", "tkout_time": "2026-07-01 01:00:00",
         "eqp_id": "EQP_01", "chamber_id": "CH", "unit_id": "2"},
        {**base, "root_lot_id": "R1", "wafer_id": "1", "tkout_time": "2026-07-01 02:00:00",
         "eqp_id": "EQP_01", "chamber_id": "CH", "unit_id": "A"},
        # W2 — 유효값이 둘이면 늦은 쪽(_9)
        {**base, "root_lot_id": "R1", "wafer_id": "2", "tkout_time": "2026-07-01 01:00:00",
         "eqp_id": "EQP_02", "chamber_id": "CH", "unit_id": "3"},
        {**base, "root_lot_id": "R1", "wafer_id": "2", "tkout_time": "2026-07-01 03:00:00",
         "eqp_id": "EQP_02", "chamber_id": "CH", "unit_id": "9"},
        # W3 — 유효값이 하나도 없음 → tkout_time last 로 대체 (행이 사라지지 않는다)
        {**base, "root_lot_id": "R1", "wafer_id": "3", "tkout_time": "2026-07-01 01:00:00",
         "eqp_id": "AUX", "chamber_id": "-", "unit_id": "-"},
        {**base, "root_lot_id": "R1", "wafer_id": "3", "tkout_time": "2026-07-01 02:00:00",
         "eqp_id": "AUX", "chamber_id": "CH", "unit_id": "B"},
    ])
    _run_fab_only(pipe, "step_desc,feature_name,agg\nGATE_ETCH,ecuall,valid_or_last\n")

    got = pl.read_parquet(pipe.feature_dir("VH_PRODA") / "FAB_GATE_ETCH_ecuall.parquet")
    val = dict(zip(got["wafer_id"], got["FAB_GATE_ETCH_ecuall"]))
    assert val["1"] == "EQP_01_CH_2"     # 무효가 더 늦어도 유효값 우선
    assert val["2"] == "EQP_02_CH_9"     # 유효값 중에서는 늦은 쪽
    assert val["3"] == "AUX_CH_B"        # 유효값 없음 → last 로 대체
    assert set(val) == {"1", "2", "3"}   # valid_eqp 와 달리 wafer 가 빠지지 않는다


def test_valid_eqp_leaves_null_when_no_valid_value(pipe):
    """대조군 — valid_eqp 는 유효값이 없으면 값을 못 채운다 (valid_or_last 와의 차이).

    번들 config/feature_funcs.py 가 같은 이름의 agg_valid_eqp 를 정의하고 있어
    커스텀이 내장보다 우선한다 — 커스텀은 그룹 안 filter 라 wafer 는 남고 값만 null,
    내장은 group_by 전 filter 라 wafer 행 자체가 사라진다.
    """
    import polars as pl
    base = {"step_desc": "GATE_ETCH", "step_id": "CC942300", "ppid": "P", "split": "s",
            "part_id": "X", "reticle_id": "-", "eqp_model": "M", "sleuth_order": "1",
            "root_lot_id": "R1", "tkout_time": "2026-07-01 01:00:00"}
    _fab_event(pipe, [
        {**base, "wafer_id": "1", "eqp_id": "EQP_01", "chamber_id": "-", "unit_id": "-"},
        {**base, "wafer_id": "9", "eqp_id": "AUX", "chamber_id": "-", "unit_id": "-"},
    ])
    # ecuall 은 고정 규칙이 걸리므로, 대조는 강제 대상이 아닌 eqp_id 로 한다
    _run_fab_only(pipe, "step_desc,feature_name,agg\nGATE_ETCH,eqp_id,valid_eqp\n")
    got = pl.read_parquet(pipe.feature_dir("VH_PRODA") / "FAB_GATE_ETCH_eqp_id.parquet")
    val = dict(zip(got["wafer_id"], got["FAB_GATE_ETCH_eqp_id"]))
    assert val["1"] == "EQP_01"
    assert val["9"] is None      # valid_or_last 였다면 "AUX" 가 채워진다


def test_sleuth_order_fills_blanks_by_wafer_id_within_step_lot(pipe):
    """빈 sleuth_order → (step_id, root_lot_id) 안에서 wafer_id 오름차순 순번."""
    import polars as pl
    base = {"step_desc": "GATE_ETCH", "ppid": "P", "split": "s", "part_id": "X",
            "reticle_id": "-", "eqp_id": "E", "eqp_model": "M",
            "chamber_id": "-", "unit_id": "-", "tkout_time": "2026-07-01 01:00:00"}
    _fab_event(pipe, [
        # R1 — 전부 빈값. "10" 이 "2" 보다 뒤여야 한다 (문자열 정렬이면 반대로 나온다)
        {**base, "step_id": "CC942300", "root_lot_id": "R1", "wafer_id": "2",
         "sleuth_order": None},
        {**base, "step_id": "CC942300", "root_lot_id": "R1", "wafer_id": "10",
         "sleuth_order": ""},
        {**base, "step_id": "CC942300", "root_lot_id": "R1", "wafer_id": "1",
         "sleuth_order": "-"},
        # R2 — 값이 있는 행은 그대로 둔다
        {**base, "step_id": "CC942300", "root_lot_id": "R2", "wafer_id": "3",
         "sleuth_order": "77"},
        {**base, "step_id": "CC942300", "root_lot_id": "R2", "wafer_id": "1",
         "sleuth_order": None},
    ])
    _run_fab_only(pipe, "step_desc,feature_name,agg\nGATE_ETCH,sleuth_order,last\n")

    got = pl.read_parquet(pipe.feature_dir("VH_PRODA") / "FAB_GATE_ETCH_sleuth_order.parquet")
    val = {(r["root_lot_id"], r["wafer_id"]): r["FAB_GATE_ETCH_sleuth_order"]
           for r in got.iter_rows(named=True)}
    assert val[("R1", "1")] == "1"
    assert val[("R1", "2")] == "2"
    assert val[("R1", "10")] == "3"     # 숫자 기준 정렬 — 문자열이면 "10" 이 2번이 된다
    assert val[("R2", "3")] == "77"     # 기존 값 보존
    assert val[("R2", "1")] == "1"      # lot 별로 다시 1번부터


def test_sleuth_order_numbers_per_step_id_not_globally(pipe):
    """순번은 step_id 별로 독립 — 같은 lot 이라도 step 이 다르면 1번부터."""
    import polars as pl
    base = {"step_desc": "GATE_ETCH", "ppid": "P", "split": "s", "part_id": "X",
            "reticle_id": "-", "eqp_id": "E", "eqp_model": "M", "chamber_id": "-",
            "unit_id": "-", "root_lot_id": "R1", "sleuth_order": None}
    _fab_event(pipe, [
        {**base, "step_id": "CC942300", "wafer_id": "5", "tkout_time": "2026-07-01 01:00:00"},
        {**base, "step_id": "CC942300", "wafer_id": "7", "tkout_time": "2026-07-01 01:00:00"},
        {**base, "step_id": "CC942301", "wafer_id": "7", "tkout_time": "2026-07-01 05:00:00"},
    ])
    _run_fab_only(pipe, "step_desc,feature_name,agg\nGATE_ETCH,sleuth_order,last\n")
    got = pl.read_parquet(pipe.feature_dir("VH_PRODA") / "FAB_GATE_ETCH_sleuth_order.parquet")
    val = dict(zip(got["wafer_id"], got["FAB_GATE_ETCH_sleuth_order"]))
    assert val["5"] == "1"
    assert val["7"] == "1"   # CC942301 에서 1번 (agg last · tkout_time 이 더 늦음)


# ── 실행 락 공유 — 잘린/구멍난 event DB 방지 ──
def test_manual_run_blocked_while_pipeline_busy(pipe):
    """수동 단건 실행이 스케줄러와 락을 공유 — 겹치면 PipelineBusy."""
    import pytest as _pytest
    from backend.core.pipeline_runner import PipelineBusy
    runner = PipelineRunner(pipe)
    assert runner._run_lock.acquire(blocking=False)   # 스케줄러가 잡고 있는 상황
    try:
        with _pytest.raises(PipelineBusy):
            runner.run_vehicle_once("VH_PRODA")
        with _pytest.raises(PipelineBusy):
            runner.run_wide_once("VH_PRODA")
        with _pytest.raises(PipelineBusy):
            runner.run_send_form_once()
    finally:
        runner._run_lock.release()
    # 락이 풀리면 정상 실행
    assert runner.run_vehicle_once("VH_PRODA")["vehicle"] == "VH_PRODA"


def test_rebuild_waits_instead_of_skipping(pipe):
    """매칭 갱신 재생성은 skip 하지 않고 기다린다 — 건너뛰면 옛 매칭 산출물이 남는다."""
    import threading
    import time as _time
    runner = PipelineRunner(pipe)
    published: list[str] = []
    runner.on_vehicle_done = lambda v, _r: published.append(v)
    pipe.run_all("VH_PRODA")

    runner._run_lock.acquire()                  # 스케줄러가 실행 중인 상황

    def release_after():
        _time.sleep(0.5)
        runner._run_lock.release()

    t = threading.Thread(target=release_after, daemon=True)
    t.start()
    t0 = _time.time()
    result = runner.rebuild_after_config_change(timeout=30)
    elapsed = _time.time() - t0
    t.join(5)

    assert elapsed >= 0.4                       # 포기하지 않고 기다렸다
    assert result["waited_sec"] >= 0.4
    assert "VH_PRODA" in result["vehicles"]     # 락이 풀린 뒤 재생성 수행
    assert "VH_PRODA" in published              # 알람 재발행 훅도 그대로 탄다
    assert runner.last_rebuild == result        # 결과가 남아야 조용한 실패가 안 된다


def test_rebuild_reports_errors_instead_of_swallowing(pipe):
    """raw 미실행 vehicle 등의 실패를 삼키지 않고 errors 로 돌려준다."""
    runner = PipelineRunner(pipe)               # event DB 없음 → run_feature 가 RuntimeError
    result = runner.rebuild_after_config_change(timeout=5)
    assert result["ok"] is False
    assert set(result["errors"]) >= set(pipe.vehicles())
    assert runner.snapshot()["last_rebuild"]["ok"] is False


def test_rebuild_times_out_rather_than_racing(pipe):
    """락을 못 잡으면 그냥 포기하고 사유를 남긴다 (동시 쓰기로 진입하지 않는다)."""
    runner = PipelineRunner(pipe)
    assert runner._run_lock.acquire(blocking=False)
    try:
        result = runner.rebuild_after_config_change(timeout=0.2)
    finally:
        runner._run_lock.release()
    assert result["ok"] is False and "시간 초과" in result["error"]


# ── 제품별 주기 + 실행 로그 ──
def _set_rpd(pipe, **rpd):
    cfg = pipe.vehicles()
    for v, n in rpd.items():
        if n is None:
            cfg[v].pop("runs_per_day", None)
        else:
            cfg[v]["runs_per_day"] = n
    pipe.save_vehicles(cfg)


def test_runs_per_day_is_per_vehicle(pipe):
    """제품마다 주기를 다르게 — 일 6회는 4시간, 일 3회는 8시간 간격."""
    runner = PipelineRunner(pipe)
    _set_rpd(pipe, VH_PRODA=6, VH_PRODB=3)
    plan = runner.schedule_plan()
    assert plan["VH_PRODA"]["interval_hours"] == 4.0
    assert plan["VH_PRODB"]["interval_hours"] == 8.0
    assert {p["source"] for p in plan.values()} == {"vehicle"}


def test_runs_per_day_falls_back_to_global_interval(pipe):
    """값이 없는 제품은 전역 interval_hours 를 따른다 (기존 동작 유지)."""
    runner = PipelineRunner(pipe)
    _set_rpd(pipe, VH_PRODA=None, VH_PRODB=None)
    cfg = pipe.global_cfg()
    cfg["runtime"]["interval_hours"] = 8      # 하루 3회
    pipe.save_global_cfg(cfg)
    plan = runner.schedule_plan()
    assert plan["VH_PRODA"]["runs_per_day"] == 3
    assert plan["VH_PRODA"]["source"] == "global"
    assert plan["VH_PRODA"]["interval_hours"] == 8.0


def test_runs_per_day_zero_excludes_from_schedule(pipe):
    """0 = 자동 실행 대상에서 제외 (수동 실행은 여전히 가능)."""
    runner = PipelineRunner(pipe)
    _set_rpd(pipe, VH_PRODA=6, VH_PRODB=0)
    plan = runner.schedule_plan()
    assert plan["VH_PRODB"]["interval_sec"] == 0 and plan["VH_PRODB"]["enabled"] is False
    assert runner.due_vehicles() == ["VH_PRODA"]


# ── 실행 금지 시간대 (quiet window) ─────────────────────────
def _at(hour, minute=0):
    """오늘 로컬 HH:MM 의 epoch."""
    import time as _time
    lt = _time.localtime()
    return _time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, hour, minute, 0, 0, 0, -1))


def _set_quiet(pipe, **kw):
    cfg = pipe.global_cfg()
    cfg["runtime"].update(kw)
    pipe.save_global_cfg(cfg)


def test_quiet_window_blocks_auto_runs(pipe):
    """금지 시간대(00:00~02:00)에는 자동 실행 대상이 비어야 한다 — 수동은 별개."""
    runner = PipelineRunner(pipe)
    _set_rpd(pipe, VH_PRODA=24, VH_PRODB=24)
    _set_quiet(pipe, quiet_enabled=True, quiet_start="00:00", quiet_end="02:00")

    assert runner.quiet_now(_at(1)) is True
    assert runner.quiet_now(_at(3)) is False
    assert runner.due_vehicles(_at(1)) == []                       # 금지 — 아무것도 안 뜬다
    assert runner.due_vehicles(_at(3)) == ["VH_PRODA", "VH_PRODB"]
    assert abs(runner.quiet_until(_at(1)) - _at(2)) < 1            # 02:00 에 해제
    assert runner.quiet_until(_at(3)) is None
    # 예정 시각은 건너뛰는 게 아니라 해제 시각으로 밀린다
    assert abs(runner.schedule_plan(_at(1))["VH_PRODA"]["next_ts"] - _at(2)) < 1
    assert runner.schedule_plan(_at(1))["VH_PRODA"]["quiet_blocked"] is True


def test_quiet_window_wraps_over_midnight(pipe):
    """시작 > 종료 = 자정을 넘는 구간 (23:00~02:00)."""
    runner = PipelineRunner(pipe)
    _set_quiet(pipe, quiet_enabled=True, quiet_start="23:00", quiet_end="02:00")
    assert runner.quiet_now(_at(23, 30)) is True
    assert runner.quiet_now(_at(1)) is True
    assert runner.quiet_now(_at(12)) is False
    # 23:30 의 해제는 "다음날" 02:00
    assert abs(runner.quiet_until(_at(23, 30)) - (_at(2) + 86400)) < 1


def test_quiet_window_off_or_invalid_never_blocks(pipe):
    """꺼져 있거나 값이 이상하면 막지 않는다 — 조용히 파이프라인이 멈추면 안 된다."""
    runner = PipelineRunner(pipe)
    _set_quiet(pipe, quiet_enabled=False, quiet_start="00:00", quiet_end="02:00")
    assert runner.quiet_now(_at(1)) is False
    _set_quiet(pipe, quiet_enabled=True, quiet_start="25:00", quiet_end="02:00")
    assert runner.quiet_window() is None and runner.quiet_now(_at(1)) is False
    _set_quiet(pipe, quiet_enabled=True, quiet_start="01:00", quiet_end="01:00")
    assert runner.quiet_window() is None          # 빈 구간 = 없는 것
    assert PipelineRunner.parse_hhmm("07:30") == 450
    assert PipelineRunner.parse_hhmm("7:5") is None
    assert PipelineRunner.parse_hhmm(540) == 540  # yaml 60진수(9:00)로 읽힌 값도 수용


def test_quiet_defaults_apply_to_installs_without_the_keys(pipe):
    """config/ 는 seed-only — 업그레이드해도 pipeline.yaml 이 안 바뀐다.
    키가 없으면 기본값(00:00~02:00)이 적용돼야 요청한 동작이 그대로 산다."""
    cfg = pipe.global_cfg()
    for k in ("quiet_enabled", "quiet_start", "quiet_end"):
        cfg["runtime"].pop(k, None)
    pipe.save_global_cfg(cfg)
    runner = PipelineRunner(pipe)
    assert runner.quiet_state()["start"] == "00:00"
    assert runner.quiet_now(_at(1)) is True
    assert runner.quiet_now(_at(5)) is False


def test_due_only_returns_vehicles_past_their_interval(pipe):
    """제품별로 독립 판정 — 자주 도는 제품만 다시 due 가 된다."""
    import time as _time
    runner = PipelineRunner(pipe)
    _set_rpd(pipe, VH_PRODA=24, VH_PRODB=1)      # 1시간 간격 vs 24시간 간격
    now = _time.time()
    runner.runs.append({"ts": now - 2 * 3600, "vehicle": "VH_PRODA", "mode": "schedule", "ok": True})
    runner.runs.append({"ts": now - 2 * 3600, "vehicle": "VH_PRODB", "mode": "schedule", "ok": True})
    assert runner.due_vehicles(now) == ["VH_PRODA"]      # PRODB 는 아직 22시간 남음
    plan = runner.schedule_plan(now)
    assert plan["VH_PRODB"]["next_ts"] == plan["VH_PRODB"]["last_ts"] + 24 * 3600


def test_last_run_survives_restart(pipe):
    """마지막 실행 시각은 로그에서 복원 — 재기동마다 전량 재실행되면 안 된다."""
    import time as _time
    now = _time.time()
    r1 = PipelineRunner(pipe)
    _set_rpd(pipe, VH_PRODA=24, VH_PRODB=24)      # 1시간 간격
    r1.runs.append({"ts": now - 60, "vehicle": "VH_PRODA", "mode": "schedule", "ok": True})
    r1.runs.append({"ts": now - 60, "vehicle": "VH_PRODB", "mode": "schedule", "ok": True})

    r2 = PipelineRunner(pipe)                     # 재기동 상당 — 메모리 상태 없음
    assert r2.due_vehicles(now) == []
    assert r2.due_vehicles(now + 3600) == ["VH_PRODA", "VH_PRODB"]


def test_run_all_accepts_vehicle_subset(pipe):
    """스케줄러는 due 인 제품만 돌린다 — 나머지는 건드리지 않는다."""
    runner = PipelineRunner(pipe)
    r = runner.run_all(mode="schedule", vehicles=["VH_PRODA"])
    assert set(r["vehicles"]) == {"VH_PRODA"}
    assert [x["vehicle"] for x in runner.runs.tail()] == ["VH_PRODA"]
    assert runner.run_all(mode="schedule", vehicles=["없는제품"])["skipped"] == "대상 제품 없음"


def test_run_log_records_every_stage(pipe):
    """raw/event/feature/wide 단계별 소요·산출이 로그에 남는다 (화면 로그의 원본)."""
    runner = PipelineRunner(pipe)
    runner.run_all(mode="schedule", vehicles=["VH_PRODA"])
    rec = runner.runs.tail(limit=1)[0]

    assert rec["vehicle"] == "VH_PRODA" and rec["ok"] and rec["mode"] == "schedule"
    assert set(rec["stages"]) == {"raw", "event", "feature", "wide"}
    assert rec["stages"]["raw"]["units"] > 0
    assert rec["stages"]["raw"]["rows"]["FAB"] > 0
    assert rec["stages"]["event"]["sources"]["FAB"]["event_rows"] > 0
    assert rec["stages"]["feature"]["counts"]["fab"] > 0
    assert rec["stages"]["feature"]["event_dates"] > 0
    assert rec["stages"]["wide"]["rows"] > 0
    assert all("sec" in rec["stages"][s] for s in ("raw", "event", "feature", "wide"))


def test_run_log_records_failure_with_reason(pipe):
    """실패도 남는다 — 어느 단계까지 갔고 왜 멈췄는지."""
    import pytest as _pytest
    runner = PipelineRunner(pipe)
    (pipe.root / "config" / "step_matching" / "vehicle_matching.csv").unlink()
    with _pytest.raises(Exception):
        runner.run_vehicle("VH_PRODA", mode="manual")
    rec = runner.runs.tail(limit=1)[0]
    assert rec["ok"] is False and rec["error"]
    assert "raw" in rec["stages"]        # raw 까지는 돌았다는 게 남아야 진단이 된다


def test_manual_run_is_logged_too(pipe):
    """수동 실행도 같은 경로 → 같은 로그. 화면에서 트리거를 구분해 볼 수 있다."""
    runner = PipelineRunner(pipe)
    runner.run_vehicle_once("VH_PRODA")
    rec = runner.runs.tail(limit=1)[0]
    assert rec["mode"] == "manual" and rec["vehicle"] == "VH_PRODA"
    assert runner.runs.vehicle_summary()["VH_PRODA"]["runs"] == 1


def test_run_log_tail_filters_and_summary(pipe):
    runner = PipelineRunner(pipe)
    for i, (v, ok) in enumerate([("VH_PRODA", True), ("VH_PRODB", False), ("VH_PRODA", True)]):
        runner.runs.append({"ts": 1000 + i, "vehicle": v, "mode": "schedule",
                            "ok": ok, "elapsed_sec": 1.0})
    assert [r["vehicle"] for r in runner.runs.tail()] == ["VH_PRODA", "VH_PRODB", "VH_PRODA"]
    assert len(runner.runs.tail(vehicle="VH_PRODA")) == 2
    assert len(runner.runs.tail(failed_only=True)) == 1
    s = runner.runs.vehicle_summary()
    assert s["VH_PRODA"]["ok"] == 2 and s["VH_PRODB"]["failed"] == 1


def test_run_log_trims_when_oversized(tmp_path):
    """로그가 커지면 최신 절반만 남긴다 — 무한히 자라지 않는다."""
    from backend.core.run_log import RunLog
    log = RunLog(tmp_path / "runs.jsonl", max_bytes=2048)
    for i in range(400):
        log.append({"ts": i, "vehicle": "V", "mode": "schedule", "ok": True, "pad": "x" * 60})
    assert log.path.stat().st_size <= 2048 * 2
    recs = log.tail(limit=10_000)
    assert recs and recs[0]["ts"] == 399          # 최신은 반드시 남는다


def test_yaml_save_preserves_header_comments(pipe):
    """웹에서 주기를 바꿔도 설정 파일의 설명 주석이 남아야 한다."""
    fp = pipe.root / "config" / "vehicles.yaml"
    fp.write_text("# 설명 1\n# 설명 2\n\nVH_X:\n  vehicle: VH_X\n  product: X\n", encoding="utf-8")
    cfg = pipe.vehicles()
    cfg["VH_X"]["runs_per_day"] = 6
    pipe.save_vehicles(cfg)

    text = fp.read_text(encoding="utf-8")
    assert text.startswith("# 설명 1\n# 설명 2\n")
    assert pipe.vehicles()["VH_X"]["runs_per_day"] == 6

    pipe.save_vehicles(pipe.vehicles())          # 반복 저장에도 주석이 늘거나 사라지지 않는다
    assert fp.read_text(encoding="utf-8").count("# 설명 1") == 1
