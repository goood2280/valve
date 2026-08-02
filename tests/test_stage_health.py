"""stage_health — 제품별 단계 정체(raw/event/feature/wide) 판정과 알람 동봉 검증.

사내 반입 초기에 실제로 겪는 상태들을 그대로 만든다:
  · 아무것도 없다 (raw 부터 안 생김)
  · raw 는 들어오는데 event 만 멈췄다
  · 전 단계가 며칠째 안 늘고 있다
"""
import json
import os
import shutil
import time
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest
import yaml

from backend.core.alert_store import ALERT_SCHEMA_VERSION, AlertStore
from backend.core.feature_pipeline import FeaturePipeline
from backend.core.s3_up import S3Uploader
from backend.core.stage_health import stage_health, stall_alerts, stall_cfg

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


def _write_partition(root: Path, day: date, rows: int = 2):
    d = root / f"date={day.isoformat()}"
    d.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"root_lot_id": ["R1"] * rows, "wafer_id": list(range(rows))}) \
        .write_parquet(d / "data.parquet")


def _rows(health, stage, source=None):
    return [r for r in health["stages"]
            if r["stage"] == stage and (source is None or r["source"] == source)]


def test_empty_db_marks_every_stage_stalled(pipe):
    """설치 직후 = raw 부터 안 생긴 상태. 조용한 게 아니라 알람이 떠야 한다."""
    h = stage_health(pipe, "VH_PRODA")
    assert h["stalled_count"] == len(h["stages"]) > 0
    raw_fab = _rows(h, "raw", "FAB")[0]
    assert raw_fab["stalled"] and "raw 파티션" in raw_fab["reason"]
    assert _rows(h, "wide")[0]["stalled"]


def test_fresh_run_is_not_stalled(pipe):
    """오늘까지 정상 수집·산출된 제품은 어느 단계도 정체가 아니다 —
    flow 발행본과 prefix 분리본까지 나와야 '끝까지 됐다' 이다."""
    pipe.run_raw_query("VH_PRODA")
    pipe.run_event("VH_PRODA")
    pipe.run_feature("VH_PRODA")
    pipe.run_wide("VH_PRODA")
    pipe.run_flow_tables()
    pipe.run_send_form()
    h = stage_health(pipe, "VH_PRODA")
    assert h["stalled"] == [], [r["reason"] for r in h["stalled"]]
    assert _rows(h, "raw", "FAB")[0]["lag_days"] == 0


def test_raw_lag_beyond_threshold_is_stalled(pipe):
    """최신 raw 가 그저께면(기본 임계 1일) 정체 — 어제까지는 정상으로 본다."""
    today = date.today()
    root = pipe.raw_dir("VH_PRODA", "FAB")
    _write_partition(root, today - timedelta(days=1))
    assert not _rows(stage_health(pipe, "VH_PRODA"), "raw", "FAB")[0]["stalled"]

    shutil.rmtree(root)
    _write_partition(root, today - timedelta(days=2))
    row = _rows(stage_health(pipe, "VH_PRODA"), "raw", "FAB")[0]
    assert row["stalled"] and row["lag_days"] == 2
    assert "2일 전" in row["reason"]


def test_event_behind_raw_is_stalled_even_when_raw_is_fresh(pipe):
    """raw 는 매일 들어오는데 event 만 멈춘 경우 — 이게 매칭 실패의 신호다."""
    today = date.today()
    for i in range(3):
        _write_partition(pipe.raw_dir("VH_PRODA", "FAB"), today - timedelta(days=i))
    _write_partition(pipe.event_dir("VH_PRODA", "FAB"), today - timedelta(days=3))

    h = stage_health(pipe, "VH_PRODA")
    assert not _rows(h, "raw", "FAB")[0]["stalled"]
    ev = _rows(h, "event", "FAB")[0]
    assert ev["stalled"] and ev["behind_of"] == "raw" and ev["behind_days"] == 3
    assert "raw 보다 3일 뒤처짐" in ev["reason"]


def test_threshold_days_is_configurable(pipe):
    today = date.today()
    _write_partition(pipe.raw_dir("VH_PRODA", "FAB"), today - timedelta(days=3))
    cfg = pipe.global_cfg()
    cfg["stall_alert"] = {"enabled": True, "threshold_days": 5, "stages": ["raw"]}
    pipe.save_global_cfg(cfg)

    h = stage_health(pipe, "VH_PRODA")
    assert {r["stage"] for r in h["stages"]} == {"raw"}   # stages 로 감시 대상 제한
    assert not _rows(h, "raw", "FAB")[0]["stalled"]       # 3일 < 임계 5일


def test_stall_cfg_defaults_when_key_missing(pipe):
    """config/ 는 seed-only — 기존 설치엔 stall_alert 키가 없다. 코드 기본값으로 돈다."""
    cfg = pipe.global_cfg()
    cfg.pop("stall_alert", None)
    pipe.save_global_cfg(cfg)
    got = stall_cfg(pipe.global_cfg())
    assert got["enabled"] and got["threshold_days"] == 1
    assert got["stages"] == ["raw", "event", "feature", "wide", "flow", "send", "s3"]


def test_disabled_produces_no_alerts(pipe):
    cfg = pipe.global_cfg()
    cfg["stall_alert"] = {"enabled": False, "threshold_days": 1, "stages": ["raw"]}
    pipe.save_global_cfg(cfg)
    assert stall_alerts(pipe, "VH_PRODA") == []


def test_downstream_lag_is_cascade_not_a_second_alarm(pipe):
    """raw 가 3일 밀리면 event 도 같이 오래된다 — 원인은 하나인데 알람이 셋이면 안 된다."""
    old = date.today() - timedelta(days=3)
    _write_partition(pipe.raw_dir("VH_PRODA", "FAB"), old)
    _write_partition(pipe.event_dir("VH_PRODA", "FAB"), old)

    h = stage_health(pipe, "VH_PRODA")
    raw = _rows(h, "raw", "FAB")[0]
    ev = _rows(h, "event", "FAB")[0]
    assert raw["stalled"] and not raw["cascade"]        # 원인
    assert ev["stalled"] and ev["cascade"]              # 여파
    assert "앞 단계 raw 가 밀린 여파" in ev["reason"]
    # 현황에는 둘 다 남지만 알람은 원인 단계만
    assert h["stalled_count"] > h["root_cause_count"]
    ids = {a["id"] for a in stall_alerts(pipe, "VH_PRODA")}
    assert "stall|VH_PRODA|raw|FAB" in ids
    assert "stall|VH_PRODA|event|FAB" not in ids


def test_event_behind_raw_stays_a_root_cause(pipe):
    """반대로 raw 는 멀쩡한데 event 만 뒤처졌으면 그건 event 자신의 문제다."""
    today = date.today()
    _write_partition(pipe.raw_dir("VH_PRODA", "FAB"), today)
    _write_partition(pipe.event_dir("VH_PRODA", "FAB"), today - timedelta(days=3))
    ev = _rows(stage_health(pipe, "VH_PRODA"), "event", "FAB")[0]
    assert ev["stalled"] and not ev["cascade"]
    assert "stall|VH_PRODA|event|FAB" in {a["id"] for a in stall_alerts(pipe, "VH_PRODA")}


def test_flow_publish_and_send_form_are_watched(pipe):
    """flow 는 db 루트의 ML_TABLE_{product} 만 보고, 최종 납품물은 prefix 분리본이다 —
    wide 까지만 보면 '내부는 최신인데 내보낸 건 옛날' 을 놓친다."""
    pipe.run_raw_query("VH_PRODA")
    pipe.run_event("VH_PRODA")
    pipe.run_feature("VH_PRODA")
    pipe.run_wide("VH_PRODA")
    h = stage_health(pipe, "VH_PRODA")

    flow = _rows(h, "flow")[0]
    assert flow["stalled"] and "db 루트에 없습니다" in flow["reason"]
    assert flow["source"] == "PRODA"
    sends = _rows(h, "send")
    assert {r["source"] for r in sends} == {"0.KNOB", "1.FAB", "2.VM", "3.INLINE"}
    assert all(r["stalled"] and r["scope"] == "global" for r in sends)

    pipe.run_flow_tables()
    pipe.run_send_form()
    h = stage_health(pipe, "VH_PRODA")
    assert not _rows(h, "flow")[0]["stalled"]
    assert not any(r["stalled"] for r in _rows(h, "send"))


def test_global_send_alert_id_has_no_vehicle(pipe):
    """SEND_FORM 은 전 제품 합산이다 — 제품 수만큼 같은 알람이 생기면 안 된다."""
    ids_a = {a["id"] for a in stall_alerts(pipe, "VH_PRODA") if a["stage"] == "send"}
    ids_b = {a["id"] for a in stall_alerts(pipe, "VH_PRODB") if a["stage"] == "send"}
    assert ids_a == ids_b == {f"stall|-|send|{g}" for g in
                              ("0.KNOB", "1.FAB", "2.VM", "3.INLINE")}


def test_stale_wide_does_not_make_every_later_stage_a_root_cause(pipe):
    """feature 가 밀려 있으면 wide·flow·send 도 같이 오래된다 — 원인은 하나여야 한다."""
    pipe.run_raw_query("VH_PRODA")
    pipe.run_event("VH_PRODA")
    pipe.run_feature("VH_PRODA")
    pipe.run_wide("VH_PRODA")
    pipe.run_flow_tables()
    pipe.run_send_form()
    # 전 산출물을 4일 전으로 되돌린다 (= 파이프라인이 4일째 안 돎)
    old = time.time() - 4 * 86400
    fdir = pipe.feature_dir("VH_PRODA")
    meta = json.loads((fdir / "_meta.json").read_text(encoding="utf-8"))
    meta["ts"] = old
    (fdir / "_meta.json").write_text(json.dumps(meta), encoding="utf-8")
    for p in [*pipe.wide_dir().rglob("*.parquet"), *pipe.send_dir().rglob("*.*"),
              *pipe.db_root().glob("ML_TABLE_*.parquet")]:
        os.utime(p, (old, old))

    h = stage_health(pipe, "VH_PRODA")
    late = [r for r in h["stages"] if r["stage"] in ("wide", "flow", "send")]
    assert all(r["stalled"] for r in late)          # 현황에는 전부 남고
    assert all(r["cascade"] for r in late), [       # 알람은 앞 단계(feature)에서만
        (r["stage"], r["reason"]) for r in late if not r["cascade"]]


def test_send_form_behind_wide_is_a_root_cause(pipe):
    """반대로 wide 는 새로 만들었는데 prefix 분리가 안 돌았으면 그건 send 자신의 문제다."""
    pipe.run_raw_query("VH_PRODA")
    pipe.run_event("VH_PRODA")
    pipe.run_feature("VH_PRODA")
    pipe.run_wide("VH_PRODA")
    pipe.run_flow_tables()
    pipe.run_send_form()
    old = time.time() - 3 * 86400
    for p in pipe.send_dir().rglob("*.*"):
        os.utime(p, (old, old))

    sends = _rows(stage_health(pipe, "VH_PRODA"), "send")
    assert all(r["stalled"] and not r["cascade"] for r in sends)
    # 뒤처짐은 내림(floor) — 3일 조금 못 미치면 2일로 센다. 애매한 반나절 차이로
    # 알람이 뜨는 것보다 하루 늦게 뜨는 쪽이 낫다.
    assert all("wide 보다" in r["reason"] and r["behind_days"] >= 2 for r in sends)


class _FakeJobs:
    """s3_jobs.list_with_status() 최소 구현 — 전송 감시만 검증한다."""

    def __init__(self, root, items):
        self.root, self._items = root, items

    def list_with_status(self):
        return {"auto_download_enabled": True, "auto_upload_enabled": True,
                "items": self._items}

    def _resolve(self, root, target):
        return self.root / target


def _up(item_id, *, target, last_status="ok", last_end=None, interval=10,
        enabled=True, configured=True):
    return {"id": item_id, "direction": "upload", "root": "db", "target": target,
            "key": f"flow/artifacts/{item_id}", "interval_min": interval,
            "enabled": enabled, "s3_configured": configured,
            "status": {"last_status": last_status,
                       "last_end": time.time() if last_end is None else last_end}}


def test_s3_upload_is_watched(pipe):
    """산출은 다 됐는데 전송이 멈추면 flow 는 옛 데이터를 계속 본다 — 다른 고장이다."""
    pipe.run_raw_query("VH_PRODA")
    pipe.run_event("VH_PRODA")
    pipe.run_feature("VH_PRODA")
    pipe.run_wide("VH_PRODA")
    pipe.run_flow_tables()
    pipe.run_send_form()
    jobs = _FakeJobs(pipe.db_root(), [
        _up("up_ml_table_PRODA", target="ML_TABLE_PRODA.parquet"),
        _up("up_stuck", target="ML_TABLE_PRODA.parquet",
            last_end=time.time() - 3 * 86400),          # 3일째 안 나감
        _up("up_broken", target="ML_TABLE_PRODA.parquet", last_status="error"),
        _up("up_manual", target="ML_TABLE_PRODA.parquet", interval=0),   # 수동 전용
        _up("up_off", target="ML_TABLE_PRODA.parquet", enabled=False),
    ])
    rows = {r["source"].split()[-1]: r
            for r in _rows(stage_health(pipe, "VH_PRODA", s3_jobs=jobs), "s3")}

    assert set(rows) == {"up_ml_table_PRODA", "up_stuck", "up_broken"}  # 수동/비활성 제외
    assert not rows["up_ml_table_PRODA"]["stalled"]
    assert rows["up_stuck"]["stalled"] and "로컬 산출물 보다" in rows["up_stuck"]["reason"]
    assert rows["up_broken"]["stalled"] and "성공한 전송이 없습니다" in rows["up_broken"]["reason"]
    assert all(r["scope"] == "global" for r in rows.values())

    ids = {a["id"] for a in stall_alerts(pipe, "VH_PRODA", s3_jobs=jobs)}
    assert "stall|-|s3|upload up_stuck" in ids


def test_s3_stage_is_skipped_when_engine_absent(pipe):
    """s3_jobs 는 app.py 가 나중에 꽂는다 — 없으면 전송 단계만 빠지고 나머지는 그대로."""
    h = stage_health(pipe, "VH_PRODA")
    assert not _rows(h, "s3")
    assert _rows(h, "raw", "FAB")


def test_stall_alert_row_shape(pipe):
    today = date.today()
    _write_partition(pipe.raw_dir("VH_PRODA", "FAB"), today - timedelta(days=4))
    rows = [a for a in stall_alerts(pipe, "VH_PRODA")
            if a["stage"] == "raw" and a["source"] == "FAB"]
    assert len(rows) == 1
    a = rows[0]
    assert a["id"] == "stall|VH_PRODA|raw|FAB"
    assert a["type"] == "stage_stall"
    assert a["product"] and a["lag_days"] == 4 and a["threshold_days"] == 1
    assert a["latest_date"] == (today - timedelta(days=4)).isoformat()


def test_alert_store_publishes_stall_and_health(pipe, tmp_path):
    """flow 가 읽는 payload 에 정체 알람 행과 health 블록이 모두 들어간다."""
    s3 = S3Uploader({"s3": {"bucket": "b", "fake_local_path": str(tmp_path / "s3_local")}})
    store = AlertStore(pipe, s3, {"alerts": {"s3_enabled": False, "outbox_dir": "s3_outbox"}},
                       tmp_path)
    store.publish("VH_PRODA")

    key = tmp_path / "s3_outbox" / "valve-alerts" / "pipeline" / "VH_PRODA.json"
    payload = json.loads(key.read_text(encoding="utf-8"))
    assert payload["schema"] == ALERT_SCHEMA_VERSION >= 3
    stalls = [a for a in payload["alerts"] if a["type"] == "stage_stall"]
    assert stalls and all(a["reason"] for a in stalls)
    assert payload["health"]["vehicle"] == "VH_PRODA"
    assert payload["health"]["stalled_count"] == len(stalls)


def test_reformatter_source_without_items_is_not_reported(pipe):
    """ET REAL 항목이 없는 vehicle 은 raw 를 안 만드는 게 정상 — 정체로 치지 않는다."""
    for f in (pipe.root / "config" / "reformatter").glob("*"):
        f.unlink()
    (pipe.root / "config" / "reformatter" / "VH_PRODA_reformatter.csv").write_text(
        "ITEMID,CATEGORY\nX,ADDP\n", encoding="utf-8")
    h = stage_health(pipe, "VH_PRODA")
    assert not _rows(h, "raw", "ET")


def test_feature_build_age_counts_as_stall(pipe):
    """feature 는 파일에 날짜 컬럼이 없다 — 마지막 산출 시각이 유일한 진행 신호다."""
    today = date.today()
    for src in ("FAB", "INLINE", "VM"):
        _write_partition(pipe.raw_dir("VH_PRODA", src), today)
        _write_partition(pipe.event_dir("VH_PRODA", src), today)
    fdir = pipe.feature_dir("VH_PRODA")
    fdir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"root_lot_id": ["R1"], "wafer_id": [1], "FAB_X": [1.0]}) \
        .write_parquet(fdir / "fab_X.parquet")
    (fdir / "_meta.json").write_text(json.dumps({
        "ts": time.time() - 3 * 86400,   # 3일 전 산출로 기록

        "sources": {s: {"days": 1, "start": today.isoformat(), "end": today.isoformat()}
                    for s in ("FAB", "INLINE", "VM")},
    }), encoding="utf-8")

    row = _rows(stage_health(pipe, "VH_PRODA"), "feature", "FAB")[0]
    assert row["stalled"] and "마지막 산출" in row["reason"]
