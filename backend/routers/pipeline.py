# -*- coding: utf-8 -*-
"""feature pipeline 라우터 — Ref 3단계(raw→event→feature) 실행/리포트 API.
프론트 '알람' 탭이 소비한다.

  POST /api/pipeline/run/{vehicle}       raw → event → feature → wide → unmatched 스캔 전체 실행
  POST /api/pipeline/wide/{vehicle}      feature 병합 wide form(ML_TABLE) 생성
  POST /api/pipeline/send-form           전 vehicle ML_TABLE 병합 → KNOB/FAB(+MASK)/VM/INLINE 분리
  GET  /api/pipeline/vehicles            vehicle 설정 목록
  GET  /api/pipeline/status              vehicle 별 처리 현황 (raw/event/feature · pending · stale)
  GET  /api/pipeline/features/{vehicle}  카테고리별(fab/knob/mask/inline/vm) feature 산출물
  GET  /api/pipeline/unmatched/{vehicle} vehicle_matching 미매칭 step 리포트 (전역 exclude 적용)
  GET  /api/pipeline/knob-miss/{vehicle} knob 화 실패(RO raw ppid) — vehicle/split 단위
  GET  /api/pipeline/sources             소스별 테이블/컬럼 설정 (FAB·INLINE·VM)
  PUT  /api/pipeline/config/sources      소스별 테이블/컬럼 저장
  PUT  /api/pipeline/config/exclude      미매칭 스캔 exclude 패턴 저장
  PUT  /api/pipeline/config/alert-cols   매칭알람 전송 열/예시 개수 저장 (알람 탭 ⚙)
  PUT  /api/pipeline/config/stall        단계 정체 알람 임계/대상 단계 저장 (알람 탭 ⚙)
  GET  /api/pipeline/health              제품별 단계 진행 현황 (raw→event→feature→wide 정체 판정)
  GET  /api/pipeline/diagnose/{vehicle}  단계별 진단 — 검사마다 열어볼 parquet(view) 동봉
  POST /api/pipeline/diagnose/{vehicle}/summary  진단 요약 (사내 LLM 있으면 문장, 없으면 규칙)
  GET  /api/pipeline/queue               작업 큐 — 실행 중/락 대기/예정
  POST /api/pipeline/queue/cancel        작업 취소 (안전 지점에서 중단)
  GET  /api/pipeline/retries             재시도 큐 (blocked = 자동 재시도 중단)
  POST /api/pipeline/retries/resume      blocked 유닛 재시도 재개
  GET  /api/pipeline/db-usage            제품별 DB 사용량 + 40GB 경고
"""
from __future__ import annotations

import time

from fastapi import APIRouter, Body, HTTPException

from backend.core import diagnose_ai
from backend.core.alert_store import AlertStore
from backend.core.csv_sync import CsvSync
from backend.core.diagnose import diagnose
from backend.core.feature_pipeline import DEFAULT_SOURCES, FeaturePipeline
from backend.core.pipeline_runner import PipelineBusy, PipelineRunner
from backend.core.runtime_env import plan_dict
from backend.core.stage_health import ALL_STAGES, stage_health, stall_cfg

router = APIRouter()

_pipe: FeaturePipeline | None = None
_alerts: AlertStore | None = None
alerts: AlertStore | None = None  # app.py 가 startup 에서 주기 발행 loop 를 제어
runner: PipelineRunner | None = None  # 병렬 실행/스케줄러 (app.py startup 에서 loop 제어)
csv_sync: CsvSync | None = None  # app.py 가 startup 에서 background loop 를 제어
s3_jobs = None  # S3 전송 엔진 (app.py 가 attach_s3_jobs 로 주입 — 정체 감시/진단이 읽는다)

# 마지막 진단 결과 (vehicle → (ts, result)). AI 요약이 화면과 같은 결과를 보게 하려는 것 —
# 클라이언트가 보낸 진단 결과를 믿지 않고, 그렇다고 요약 때마다 parquet 을 다시 읽지도 않는다.
_last_diag: dict = {}
_DIAG_CACHE_SEC = 600


def deps(root, settings, s3_uploader, lake_api=None):
    global _pipe, _alerts, alerts, runner, csv_sync
    _pipe = FeaturePipeline(root, settings, lake_api=lake_api)
    _alerts = AlertStore(_pipe, s3_uploader, settings, root)
    alerts = _alerts
    runner = PipelineRunner(_pipe)
    runner.on_vehicle_done = lambda v, _r: _alerts.publish(v)  # 실행 후 알람 S3 발행
    csv_sync = CsvSync(root, s3_uploader)
    csv_sync.on_updated = _refresh_after_sync   # legacy 경로 (s3_jobs 미이관 환경)


def attach_s3_jobs(jobs) -> None:
    """S3 전송 엔진을 알람/진단에 연결 (app.py startup — s3jobs 가 나중에 생긴다).

    로컬 단계가 전부 최신이어도 업로드가 멈추면 flow 는 옛 데이터를 계속 본다 —
    전송도 감시 대상이라 정체 판정과 진단이 이 참조를 쓴다."""
    global s3_jobs
    s3_jobs = jobs
    if _alerts is not None:
        _alerts.s3_jobs = jobs


def on_config_downloaded(paths: list) -> None:
    """S3 에서 매칭 csv 를 새로 받았을 때 호출 (s3_jobs · csv_sync 공용 훅).
    config/feature_rules · step_matching 밑이 바뀐 경우에만 재생성한다 —
    settings.json 같은 건 event/feature 와 무관하다."""
    hit = [p for p in paths
           if any(k in str(p).replace("\\", "/")
                  for k in ("feature_rules", "step_matching", "fab_scan", "reformatter"))]
    if hit:
        _refresh_after_sync(hit)


def _refresh_after_sync(_dests: list):
    """csv 동기화로 매칭 파일이 갱신되면 전 vehicle event/feature/wide 재생성.
    (run_event 가 설정 버전(event_version) 비교로 필요한 소스만 전체 rebuild) + 알람 재발행.

    runner 의 실행 락을 공유한다 — 스케줄/수동 실행과 겹치면 같은 event 디렉터리를
    동시에 rmtree/write 해서 파티션이 유실되거나 잘린 parquet 이 남는다.
    결과는 runner.last_rebuild + /api/pipeline/progress 로 노출."""
    result = runner.rebuild_after_config_change()
    if not result.get("ok"):
        print(f"[valve] 매칭 갱신 재생성 일부 실패: {result.get('errors') or result.get('error')}")


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


@router.get("/api/pipeline/runtime")
def runtime():
    """호스트 자원으로 산정한 워커 계획 + runtime 설정. UI 표시/조정용."""
    cfg = _p().global_cfg().get("runtime") or {}
    return {"plan": plan_dict(cfg), "config": cfg,
            "scheduler_running": runner.schedule_enabled(), "loop_running": runner.loop_enabled(),
            "quiet": runner.quiet_state()}


@router.put("/api/pipeline/runtime")
def put_runtime(body: dict = Body(...)):
    """runtime 설정 저장 (raw_days/split_days/max_workers/interval_hours/
    schedule_enabled/loop_enabled/loop_gap_sec/quiet_* 등)."""
    cfg = _p().global_cfg()
    rt = cfg.get("runtime") or {}
    for k in ("raw_days", "split_days", "raw_api_max", "max_workers", "vehicle_workers",
              "feature_workers", "mem_per_worker_gb", "cpu_cores", "interval_hours",
              "schedule_enabled", "loop_enabled", "loop_gap_sec", "sched_tick_sec",
              "quiet_enabled"):
        if k in body:
            rt[k] = body[k]
    # 실행 금지 시간대 — 형식이 틀린 값을 저장하면 조용히 무시되는 설정이 되어버린다.
    for k in ("quiet_start", "quiet_end"):
        if k in body:
            val = str(body[k] or "").strip()
            if val and PipelineRunner.parse_hhmm(val) is None:
                raise HTTPException(400, f"{k}: 'HH:MM' 형식이어야 합니다 (예 00:00)")
            rt[k] = val
    cfg["runtime"] = rt
    _p().save_global_cfg(cfg)
    return {"ok": True, "config": rt, "plan": plan_dict(rt), "quiet": runner.quiet_state()}


@router.get("/api/pipeline/schedule")
def pipeline_schedule():
    """제품별 실행 주기 현황 — runs_per_day · 다음 실행 예정 · 최근 실행 요약."""
    return {"master_enabled": bool((_p().global_cfg().get("runtime") or {}).get("schedule_enabled")),
            "default_runs_per_day": runner.default_runs_per_day(),
            "vehicles": runner.schedule_plan(),
            "quiet": runner.quiet_state(),
            "summary": runner.runs.vehicle_summary()}


@router.put("/api/pipeline/schedule/{vehicle}")
def put_pipeline_schedule(vehicle: str, body: dict = Body(...)):
    """제품 주기 저장 — {runs_per_day: 3} = 하루 3회(8시간 간격).
    0 이면 자동 실행 제외, null 이면 전역 interval_hours 를 따른다."""
    cfg = _p().vehicles()
    if vehicle not in cfg:
        raise HTTPException(404, f"{vehicle} not found in vehicles.yaml")
    rpd = body.get("runs_per_day")
    if rpd is None or str(rpd).strip() == "":
        cfg[vehicle].pop("runs_per_day", None)      # 전역 주기를 따름
    else:
        try:
            rpd = float(rpd)
        except (TypeError, ValueError):
            raise HTTPException(400, "runs_per_day 는 숫자")
        if rpd < 0 or rpd > 48:
            raise HTTPException(400, "runs_per_day 는 0~48 (0=자동 실행 안 함)")
        cfg[vehicle]["runs_per_day"] = int(rpd) if rpd == int(rpd) else rpd
    _p().save_vehicles(cfg)
    return {"ok": True, "vehicle": vehicle, "schedule": runner.schedule_plan().get(vehicle)}


@router.get("/api/pipeline/runs")
def pipeline_runs(vehicle: str = "", limit: int = 50, failed_only: bool = False,
                  severity: str = ""):
    """제품별 실행 로그 — 단계(raw/event/feature/wide)별 소요·산출·실패 사유."""
    return {"runs": runner.runs.tail(limit=limit, vehicle=vehicle or None,
                                     failed_only=failed_only, severity=severity or None),
            "summary": runner.runs.vehicle_summary(),
            "retry": runner.retries.summary(vehicle or None)}


@router.get("/api/pipeline/retries")
def pipeline_retries(vehicle: str = ""):
    """재시도 큐 — 실패한 (source×날짜) 유닛. blocked 는 자동 재시도가 멈춘 것."""
    return runner.retries.summary(vehicle or None)


@router.post("/api/pipeline/retries/resume")
def pipeline_retries_resume(body: dict = Body(default={})):
    """blocked 유닛을 다시 자동 재시도 대상으로 — {vehicle?, key?}.
    (자격증명 만료 등 원인을 고친 뒤 누르는 버튼)"""
    n = runner.retries.resume(vehicle=(body.get("vehicle") or None),
                              key=(body.get("key") or None))
    return {"ok": True, "resumed": n, "retry": runner.retries.summary()}


# ── 작업 큐 (실행 중 · 락 대기 · 예정) ──
@router.get("/api/pipeline/queue")
def pipeline_queue():
    """무엇이 돌고 있고, 무엇이 락을 기다리며, 다음에 무엇이 예정인지."""
    return runner.queue()


@router.post("/api/pipeline/queue/cancel")
def pipeline_queue_cancel(body: dict = Body(...)):
    """작업 취소 — {id}. 대기 중이면 즉시, 실행 중이면 다음 안전 지점(유닛/단계 경계)에서.
    실행 중인 event/feature 쓰기를 중간에 끊지는 않는다 (파티션이 반만 남는다)."""
    task_id = str(body.get("id") or "").strip()
    if not task_id:
        raise HTTPException(400, "id 필요")
    r = runner.cancel_task(task_id)
    if not r.get("ok"):
        raise HTTPException(409, r.get("error") or "취소 불가")
    return r


@router.get("/api/pipeline/db-usage")
def pipeline_db_usage(refresh: bool = False):
    """제품별 DB 사용량 + 경고 임계(runtime.db_warn_gb, 기본 40GB). 기본 10분 캐시."""
    return _p().db_usage(force=refresh)


@router.get("/api/pipeline/progress")
def progress():
    """실행 중 진행상황 — vehicle 별 현재 단계(raw/event/feature)·유닛 진척.
    백필이 지금 어느 DB 단계를 처리 중인지 실시간 표시용."""
    return runner.snapshot()


@router.post("/api/pipeline/run-all")
def run_all_parallel():
    """전 vehicle 병렬 raw→event→feature 1회 실행 (수동 트리거). 실행 중이면 skip."""
    return runner.run_all(mode="manual")


@router.get("/api/pipeline/config")
def get_config():
    return _p().global_cfg()


@router.get("/api/pipeline/sources")
def get_sources():
    """소스별 테이블/조회 컬럼 + 파티션 기준 열(명시값과 실제 적용값)."""
    return _p().sources_view()


@router.put("/api/pipeline/config/sources")
def put_sources(body: dict = Body(...)):
    """소스별 테이블/조회 컬럼/파티션 기준 열 저장 —
    {FAB: {table, columns: [...], time_col?}, INLINE: …} (보낸 소스만 갱신).

    time_col 은 raw 를 date= 파티션으로 나누는 기준 열. 조회 컬럼에 없는 열을
    저장하려 하면 400 — 통과시키면 하루치가 통째로 엉뚱한 파티션에 들어간다.
    빈 문자열이면 자동 인식으로 되돌린다. event/reformatter/match 등 UI 가 모르는
    키는 그대로 보존한다 (예전엔 저장 한 번에 지워졌다)."""
    cfg = _p().global_cfg()
    cur = dict(cfg.get("sources") or {})
    for name, src in (body or {}).items():
        if not isinstance(src, dict):
            continue
        entry = dict(cur.get(name) or {})
        cols = [str(c).strip() for c in (src.get("columns") or []) if str(c).strip()]
        if not cols:
            raise HTTPException(400, f"{name}: columns 가 비어있음")
        entry["columns"] = cols
        entry["table"] = str(src.get("table")
                             or entry.get("table")
                             or (DEFAULT_SOURCES.get(name) or {}).get("table")
                             or f"RAW_{name}_DATA").strip()
        if "time_col" in src:
            tcol = str(src.get("time_col") or "").strip()
            if tcol and tcol not in cols:
                raise HTTPException(
                    400, f"{name}: 파티션 기준 열 '{tcol}' 이 조회 컬럼에 없습니다 "
                         f"— 컬럼에 먼저 추가하세요")
            if tcol:
                entry["time_col"] = tcol
            else:
                entry.pop("time_col", None)     # 자동 인식으로 되돌림
        cur[name] = entry
    cfg["sources"] = cur
    _p().save_global_cfg(cfg)
    return {"ok": True, "sources": _p().sources_view()}


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


# 알람 행에 이미 고정으로 존재하는 키 — 전송 열 이름으로 쓰면 값이 덮이거나
# flow 화면·ack 로직이 깨진다.
RESERVED_ALERT_COLS = {
    "id", "type", "vehicle", "product", "step_id", "step_desc", "ppid", "split",
    "rows", "n_lots", "status", "note", "ack_note", "first_seen_ts",
    "last_seen_ts", "examples", "decision", "excluded_by",
}


@router.put("/api/pipeline/config/alert-cols")
def put_alert_cols(body: dict = Body(...)):
    """매칭알람 전송 열 저장 — {cols: [...], example_limit: n} (알람 탭 ⚙).

    cols 는 FAB raw 컬럼명 — raw 에 없는 열은 스캔 때 조용히 빠진다.
    예시 (root_lot_id, wafer_id) 쌍은 항상 전송되며 example_limit 개까지."""
    cols: list[str] = []
    for c in (body.get("cols") or []):
        c = str(c).strip()
        if not c or c in cols:
            continue
        if c in RESERVED_ALERT_COLS:
            raise HTTPException(400, f"예약된 열 이름: {c}")
        cols.append(c)
    try:
        limit = max(1, min(20, int(body.get("example_limit") or 3)))
    except (TypeError, ValueError):
        raise HTTPException(400, "example_limit 은 숫자")
    cfg = _p().global_cfg()
    us = cfg.setdefault("unmatched_scan", {})
    us["alert_cols"] = cols
    us["example_limit"] = limit
    _p().save_global_cfg(cfg)
    return {"ok": True, "alert_cols": cols, "example_limit": limit}


@router.put("/api/pipeline/config/alert-hint")
def put_alert_hint(body: dict = Body(...)):
    """function step 추천 컨텍스트 저장 — {enabled, days, neighbors, cols, value_limit}
    (알람 탭 ⚙ · pipeline.yaml unmatched_scan.hint).

    cols 는 FAB raw 컬럼명 — raw 에 없는 열은 스캔 때 조용히 빠진다
    (area 를 쓰려면 sources.FAB.columns 에 먼저 추가)."""
    cols: list[str] = []
    for c in (body.get("cols") or []):
        c = str(c).strip()
        if c and c not in cols:
            cols.append(c)
    if not cols:
        raise HTTPException(400, "비교할 열을 하나 이상 지정하세요")
    hint = {"enabled": bool(body.get("enabled", True)), "cols": cols}
    for key, lo, hi, dflt in (("days", 1, 90, 7), ("neighbors", 1, 10, 3),
                              ("value_limit", 1, 50, 12)):
        try:
            hint[key] = max(lo, min(hi, int(body.get(key) or dflt)))
        except (TypeError, ValueError):
            raise HTTPException(400, f"{key} 는 숫자")
    cfg = _p().global_cfg()
    cfg.setdefault("unmatched_scan", {})["hint"] = hint
    _p().save_global_cfg(cfg)
    return {"ok": True, "hint": hint}


@router.put("/api/pipeline/config/stall")
def put_stall_cfg(body: dict = Body(...)):
    """단계 정체 알람 설정 저장 — {enabled, threshold_days, stages}
    (알람 탭 ⚙ · pipeline.yaml stall_alert)."""
    stages = []
    for s in (body.get("stages") or []):
        s = str(s).strip().lower()
        if s and s in ALL_STAGES and s not in stages:
            stages.append(s)
    if not stages:
        raise HTTPException(400, "감시할 단계를 하나 이상 지정하세요")
    try:
        thr = max(1, min(60, int(body.get("threshold_days") or 1)))
    except (TypeError, ValueError):
        raise HTTPException(400, "threshold_days 는 숫자")
    stall = {"enabled": bool(body.get("enabled", True)),
             "threshold_days": thr,
             "stages": [s for s in ALL_STAGES if s in stages]}
    cfg = _p().global_cfg()
    cfg["stall_alert"] = stall
    _p().save_global_cfg(cfg)
    return {"ok": True, "stall_alert": stall}


@router.get("/api/pipeline/diagnose/{vehicle}")
def diagnose_vehicle(vehicle: str):
    """단계별 진단 — raw 가 되는가 → event 가 정확한가 → feature/매칭테이블이 오는가.

    각 검사는 그 자리에서 열어볼 parquet(view: root/file/sql)을 같이 돌려준다 —
    진단 탭이 그 값으로 탐색기를 열어 사람이 눈으로 확인한다."""
    p = _p()
    if vehicle not in p.vehicles():
        raise HTTPException(404, f"알 수 없는 vehicle: {vehicle}")
    result = diagnose(p, vehicle, s3_jobs=s3_jobs, csv_sync=csv_sync)
    _last_diag[vehicle] = (time.time(), result)
    return result


@router.post("/api/pipeline/diagnose/{vehicle}/summary")
def diagnose_summary(vehicle: str):
    """진단 결과 한 덩어리 요약 — 사내 LLM 이 연결돼 있으면 문장으로, 아니면 규칙으로.

    **AI 없이도 답이 나온다** (`source: "rules"`). 요약 재료는 방금 화면이 받은
    진단 결과를 그대로 쓰되, 클라이언트가 보낸 값을 믿지 않고 서버 캐시에서
    꺼낸다. 캐시가 낡았으면 다시 검사한다 — 진단은 parquet 조회라 공짜가 아니다."""
    p = _p()
    if vehicle not in p.vehicles():
        raise HTTPException(404, f"알 수 없는 vehicle: {vehicle}")
    ts, cached = _last_diag.get(vehicle) or (0.0, None)
    if cached is None or time.time() - ts > _DIAG_CACHE_SEC:
        cached = diagnose(p, vehicle, s3_jobs=s3_jobs, csv_sync=csv_sync)
        _last_diag[vehicle] = (time.time(), cached)
        ts = time.time()
    out = diagnose_ai.summarize(cached)
    out["diagnosed_at"] = ts
    return out


@router.get("/api/pipeline/health")
def health():
    """제품별 단계 진행 현황(raw→event→feature→wide) — 정체 판정 근거 그대로.
    매칭알람 payload 의 health 블록과 같은 계산이다 (backend/core/stage_health.py)."""
    p = _p()
    return {"config": stall_cfg(p.global_cfg()),
            "vehicles": {v: stage_health(p, v, s3_jobs=s3_jobs) for v in p.vehicles()}}


@router.post("/api/pipeline/wide/{vehicle}")
def wide(vehicle: str):
    """vehicle 의 feature 전부를 ML_TABLE 로 병합 → 4.WIDE_FORM."""
    try:
        return runner.run_wide_once(vehicle)
    except RuntimeError as e:     # PipelineBusy 포함 — 둘 다 409
        raise HTTPException(409, str(e))
    except ValueError as e:
        raise HTTPException(404, str(e))


@router.post("/api/pipeline/send-form")
def send_form():
    """전 vehicle ML_TABLE 병합 → prefix 그룹(0.KNOB/1.FAB(+MASK)/2.VM/3.INLINE) 분리 저장."""
    try:
        return runner.run_send_form_once()
    except RuntimeError as e:
        raise HTTPException(409, str(e))


@router.post("/api/pipeline/run/{vehicle}")
def run(vehicle: str):
    """단건 실행 — 스케줄/재생성과 실행 락을 공유한다 (동시 쓰기로 event 파티션이
    유실되지 않도록). 다른 실행이 진행 중이면 409."""
    try:
        result = runner.run_vehicle_once(vehicle)
    except PipelineBusy as e:
        raise HTTPException(409, str(e))
    except ValueError as e:
        raise HTTPException(404, str(e))
    result["published"] = _alerts.publish(vehicle)  # 활성 알람 S3 발행 → flow 가 소비
    return result


# ── 통합 알람 (미매칭 step + RO ppid, ack 상태 병합) ──
@router.get("/api/pipeline/alerts")
def alerts():
    return _alerts.list_alerts()


@router.get("/api/pipeline/alerts/outbox")
def alerts_outbox():
    """S3 업로드 폴더 현황 — 이 폴더 하나만 sync 하면 flow 매칭알람이 갱신된다.
    (트리가 S3 key 와 1:1 — sync_dir → s3://{bucket}/{s3_prefix})"""
    return _alerts.outbox_status()


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
