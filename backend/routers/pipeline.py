# -*- coding: utf-8 -*-
"""feature pipeline 라우터 — Ref 3단계(raw→event→feature) 실행/리포트 API.
프론트 '알람' 탭이 소비한다.

  POST /api/pipeline/run/{vehicle}       raw → event → feature → unmatched 스캔 전체 실행
  GET  /api/pipeline/vehicles            vehicle 설정 목록
  GET  /api/pipeline/status              vehicle 별 처리 현황 (raw/event/feature · pending · stale)
  GET  /api/pipeline/features/{vehicle}  카테고리별(fab/knob/mask/inline/vm) feature 산출물
  GET  /api/pipeline/unmatched/{vehicle} vehicle_matching 미매칭 step 리포트 (전역 exclude 적용)
  GET  /api/pipeline/knob-miss/{vehicle} knob 화 실패(RO raw ppid) — vehicle/split 단위
  GET  /api/pipeline/sources             소스별 테이블/컬럼 설정 (FAB·INLINE·VM)
  PUT  /api/pipeline/config/sources      소스별 테이블/컬럼 저장
  PUT  /api/pipeline/config/exclude      미매칭 스캔 exclude 패턴 저장
"""
from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException

from backend.core.alert_store import AlertStore
from backend.core.csv_sync import CsvSync
from backend.core.feature_pipeline import DEFAULT_SOURCES, FeaturePipeline

router = APIRouter()

_pipe: FeaturePipeline | None = None
_alerts: AlertStore | None = None
csv_sync: CsvSync | None = None  # app.py 가 startup 에서 background loop 를 제어


def deps(root, settings, s3_uploader):
    global _pipe, _alerts, csv_sync
    _pipe = FeaturePipeline(root, settings)
    _alerts = AlertStore(_pipe, s3_uploader, settings, root)
    csv_sync = CsvSync(root, s3_uploader)
    csv_sync.on_updated = _refresh_after_sync


def _refresh_after_sync(_dests: list):
    """csv 동기화로 매칭 파일이 갱신되면 전 vehicle event/feature 재생성.
    (run_event 가 sha 비교로 필요한 소스만 전체 rebuild) + 알람 재발행."""
    for v in _pipe.vehicles():
        try:
            _pipe.run_event(v)
            _pipe.run_feature(v)
            _alerts.publish(v)
        except Exception:
            continue  # raw 미실행 vehicle 등은 skip


def _p() -> FeaturePipeline:
    if _pipe is None:
        raise HTTPException(500, "pipeline not initialized")
    return _pipe


@router.get("/api/pipeline/vehicles")
def vehicles():
    return _p().vehicles()


@router.get("/api/pipeline/status")
def status():
    return {v: _p().status(v) for v in _p().vehicles()}


@router.get("/api/pipeline/config")
def get_config():
    return _p().global_cfg()


@router.get("/api/pipeline/sources")
def get_sources():
    return _p().sources_cfg()


@router.put("/api/pipeline/config/sources")
def put_sources(body: dict = Body(...)):
    """소스별 테이블/컬럼 저장 — {FAB: {table, columns: [...]}, INLINE: …, VM: …}"""
    out = {}
    for name in DEFAULT_SOURCES:
        src = body.get(name) or {}
        cols = [str(c).strip() for c in (src.get("columns") or []) if str(c).strip()]
        if not cols:
            raise HTTPException(400, f"{name}: columns 가 비어있음")
        out[name] = {"table": str(src.get("table") or DEFAULT_SOURCES[name]["table"]).strip(),
                     "columns": cols}
    _p().save_sources_cfg(out)
    return {"ok": True, "sources": out}


@router.put("/api/pipeline/config/exclude")
def put_exclude(body: dict = Body(...)):
    """미매칭 스캔 제외 패턴 저장 — {eqp_id: [...], eqp_model: [...]}"""
    cfg = _p().global_cfg()
    cfg.setdefault("unmatched_scan", {})["exclude"] = {
        "eqp_id": [str(x).strip() for x in (body.get("eqp_id") or []) if str(x).strip()],
        "eqp_model": [str(x).strip() for x in (body.get("eqp_model") or []) if str(x).strip()],
    }
    _p().save_global_cfg(cfg)
    return {"ok": True, "exclude": cfg["unmatched_scan"]["exclude"]}


@router.post("/api/pipeline/run/{vehicle}")
def run(vehicle: str):
    try:
        result = _p().run_all(vehicle)
    except ValueError as e:
        raise HTTPException(404, str(e))
    result["published"] = _alerts.publish(vehicle)  # 활성 알람 S3 발행 → flow 가 소비
    return result


# ── 통합 알람 (미매칭 step + RO ppid, ack 상태 병합) ──
@router.get("/api/pipeline/alerts")
def alerts():
    return _alerts.list_alerts()


@router.put("/api/pipeline/alerts/ack")
def alerts_ack(body: dict = Body(...)):
    """알람 상태 변경 — {id, status: 'active'|'미확인예정'|'반영불필요', note?}.
    S3 ack.json 에 기록되어 flow 와 공유. 비활성 상태는 재알람 억제."""
    alert_id = str(body.get("id") or "").strip()
    if not alert_id:
        raise HTTPException(400, "id 필요")
    _alerts.set_ack(alert_id, str(body.get("status") or "active"),
                    note=str(body.get("note") or ""))
    return {"ok": True}


# ── csv 설정파일 S3 동기화 (flow → Valve) ──
@router.get("/api/pipeline/csv-sync")
def csv_sync_info():
    return {"config": csv_sync.load_config(), "status": csv_sync.load_status()}


@router.put("/api/pipeline/csv-sync/config")
def csv_sync_save(body: dict = Body(...)):
    return {"ok": True, "config": csv_sync.save_config(body)}


@router.post("/api/pipeline/csv-sync/run")
def csv_sync_run():
    return {"results": csv_sync.sync_now()}


@router.get("/api/pipeline/features/{vehicle}")
def features(vehicle: str):
    return _p().list_features(vehicle)


@router.get("/api/pipeline/unmatched/{vehicle}")
def unmatched(vehicle: str):
    try:
        return _p().scan_unmatched(vehicle)
    except RuntimeError as e:
        raise HTTPException(409, str(e))
    except ValueError as e:
        raise HTTPException(404, str(e))


@router.get("/api/pipeline/knob-miss/{vehicle}")
def knob_miss(vehicle: str):
    return _p().load_report(vehicle, "knob_miss") or []
