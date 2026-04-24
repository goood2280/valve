"""jobs 라우터 — plan/chunk 상태·SSE·enqueue·cancel·retry·history."""
from __future__ import annotations

import asyncio
import json
from collections import deque
from datetime import date, timedelta
from pathlib import Path

from fastapi import APIRouter, HTTPException
from sse_starlette.sse import EventSourceResponse

router = APIRouter(prefix="/api/jobs", tags=["jobs"])

_state = None
_executor = None
_planner = None
_products = None
_settings = None
_log_path: Path = None


def deps(state, executor, planner, products, settings, log_path=None):
    global _state, _executor, _planner, _products, _settings, _log_path
    _state = state
    _executor = executor
    _planner = planner
    _products = products
    _settings = settings
    _log_path = Path(log_path) if log_path else None


def _find_product(name: str):
    return next((p for p in _products["products"] if p["product"] == name), None)


def _find_source(prod_cfg, src_name: str):
    return next((s for s in (prod_cfg.get("sources") or []) if s["name"] == src_name), None)


@router.get("/state")
def get_state():
    """snapshot — plans · chunks · partitions."""
    return _state.snapshot()


@router.get("/stream")
async def stream_events():
    """SSE — snapshot + incremental updates."""
    q = _state.subscribe()

    async def gen():
        try:
            yield {"event": "snapshot", "data": json.dumps(_state.snapshot(), default=str)}
            while True:
                evt = await q.get()
                yield {"event": "update", "data": json.dumps(evt, default=str)}
        finally:
            _state.unsubscribe(q)

    return EventSourceResponse(gen())


@router.post("/enqueue")
async def enqueue(req: dict):
    """req = {product, source, date}  하나의 (제품·소스·날짜) plan 을 즉시 실행."""
    prod = req.get("product")
    src = req.get("source")
    d = req.get("date")
    if not (prod and src and d):
        raise HTTPException(400, "product, source, date required")

    prod_cfg = _find_product(prod)
    if not prod_cfg:
        raise HTTPException(404, f"product {prod!r} not found")
    src_cfg = _find_source(prod_cfg, src)
    if not src_cfg:
        raise HTTPException(404, f"source {src!r} not found")

    plan = await _planner.build_plan(prod, src_cfg, prod_cfg, d)
    asyncio.create_task(_executor.run_plan(plan, prod_cfg, src_cfg))
    return {"plan_id": plan.plan_id, "chunks": len(plan.chunks),
            "probe_meta": plan.probe_meta}


@router.post("/enqueue-all")
async def enqueue_all():
    """스케줄 기반 일괄 enqueue — backfill_days 창 안의 모든 (제품 × 소스 × 날짜).
    제품별 backfill_days_override 가 설정돼 있으면 그 값 적용."""
    global_bf = int(_settings["schedule"].get("backfill_days", 3))
    today = date.today()

    launched = 0
    for p in _products["products"]:
        if not p.get("enabled", True):
            continue
        p_bf = int(p.get("backfill_days_override") or 0) or global_bf
        dates = [(today - timedelta(days=i)).isoformat() for i in range(p_bf + 1)]
        for s in p.get("sources", []):
            for d in dates:
                plan = await _planner.build_plan(p["product"], s, p, d)
                asyncio.create_task(_executor.run_plan(plan, p, s))
                launched += 1
    return {"launched": launched, "backfill_days": global_bf}


@router.post("/enqueue-product")
async def enqueue_product(req: dict):
    """제품 단위 일괄 enqueue — 신규 제품의 초기 시딩(300·600일 등) 수동 실행용.
    req = {product, days?(optional override)}. days 지정 시 해당 기간, 아니면
    product.backfill_days_override 또는 전역 backfill_days."""
    name = (req or {}).get("product")
    if not name:
        raise HTTPException(400, "product required")
    p = _find_product(name)
    if not p:
        raise HTTPException(404, f"product {name!r} not found")
    global_bf = int(_settings["schedule"].get("backfill_days", 3))
    override = req.get("days")
    if override is not None:
        p_bf = int(override)
    else:
        p_bf = int(p.get("backfill_days_override") or 0) or global_bf
    p_bf = max(1, min(p_bf, 3650))  # clamp 1~10년
    today = date.today()
    dates = [(today - timedelta(days=i)).isoformat() for i in range(p_bf + 1)]

    launched = 0
    for s in p.get("sources", []):
        for d in dates:
            plan = await _planner.build_plan(p["product"], s, p, d)
            asyncio.create_task(_executor.run_plan(plan, p, s))
            launched += 1
    return {"launched": launched, "product": name, "backfill_days": p_bf,
            "source_count": len(p.get("sources", []))}


@router.post("/cancel")
def cancel(req: dict):
    chunk_id = req.get("chunk_id")
    if not chunk_id:
        raise HTTPException(400, "chunk_id required")
    _executor.cancel(chunk_id)
    return {"ok": True, "chunk_id": chunk_id}


@router.post("/retry-partition")
async def retry_partition(req: dict):
    """(product, source, date) 재실행 — plan 다시 만들고 execute."""
    prod = req.get("product"); src = req.get("source"); d = req.get("date")
    if not (prod and src and d):
        raise HTTPException(400, "product, source, date required")
    prod_cfg = _find_product(prod)
    src_cfg = _find_source(prod_cfg, src) if prod_cfg else None
    if not (prod_cfg and src_cfg):
        raise HTTPException(404, "product/source not found")
    plan = await _planner.build_plan(prod, src_cfg, prod_cfg, d)
    asyncio.create_task(_executor.run_plan(plan, prod_cfg, src_cfg))
    return {"ok": True, "plan_id": plan.plan_id}


@router.post("/probe-invalidate")
def probe_invalidate(req: dict):
    """probe 캐시 수동 무효화. req = {product?, source?} (둘 다 없으면 전체)."""
    _planner.invalidate(req.get("product"), req.get("source"))
    return {"ok": True}


@router.get("/s3-pending")
def s3_pending():
    """S3 업로드 대기 큐 상태."""
    from backend.core import s3_queue
    return {"pending": s3_queue.pending(), "count": len(s3_queue.pending())}


@router.post("/s3-flush")
async def s3_flush():
    """대기 큐를 즉시 플러시 (manual 모드 또는 interval 모드에서 기다리지 않고 실행).
    반환: {uploaded, failed, skipped, pending}."""
    from backend.core import s3_queue
    return await s3_queue.flush_once()


@router.get("/history")
def history(limit: int = 300, product: str = "", source: str = "",
            status: str = "", failed_only: bool = False, kind: str = "chunk"):
    """실행 이력: jobs.jsonl 뒤에서부터 tail 하며 필터링.
    - kind: 'chunk'(기본) · 'plan' · 'partition' · 'all'
    - failed_only: failed / timeout_reshard / completeness_failed / upload_failed 만
    - status: 특정 상태 1개로 필터 (정확 일치). failed_only 와 동시 사용 가능
    반환은 최신순 (내림차순), 시도 시간/상태/에러 타입/메시지/duration 포함.
    """
    if not _log_path or not _log_path.exists():
        return {"items": [], "total_scanned": 0, "log_exists": False}

    limit = max(1, min(int(limit or 300), 5000))
    kind_filter = None if (kind or "").lower() == "all" else (kind or "chunk").lower()
    FAIL_STATUSES = {"failed", "timeout_reshard", "completeness_failed", "upload_failed"}

    chunk_meta: dict = {}   # chunk_id -> 가장 최근 status/update (tail 에서 먼저 본 것)
    items: list = []
    seen_chunks = set()
    total_scanned = 0

    # 뒤에서부터 라인 읽기 (파일이 크지 않다고 가정; 커지면 read reverse 최적화)
    try:
        with open(_log_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except Exception as e:
        raise HTTPException(500, f"log read error: {e}")

    for raw in reversed(lines):
        total_scanned += 1
        raw = raw.strip()
        if not raw:
            continue
        try:
            evt = json.loads(raw)
        except Exception:
            continue

        k = evt.get("kind")
        if kind_filter and k != kind_filter:
            continue

        if k == "chunk":
            cid = evt.get("chunk_id")
            upd = evt.get("update") or {}
            st = upd.get("status")
            # 한 chunk 의 최신 update 만 1개
            if cid in seen_chunks:
                continue
            if not st:
                continue  # pending-only 업데이트 무시

            if product and upd.get("product") != product:
                continue
            if source and upd.get("source") != source:
                continue
            if status and st != status:
                continue
            if failed_only and st not in FAIL_STATUSES:
                continue

            item = {
                "ts": evt.get("ts"),
                "kind": "chunk",
                "chunk_id": cid,
                "product": upd.get("product"),
                "source": upd.get("source"),
                "date": upd.get("date"),
                "status": st,
                "started_at": upd.get("started_at"),
                "ended_at": upd.get("ended_at"),
                "duration_sec": upd.get("duration_sec"),
                "expected_rows": upd.get("expected_rows"),
                "actual_rows": upd.get("actual_rows"),
                "error_type": upd.get("error_type"),
                "error": upd.get("error"),
                "shard_filters": upd.get("shard_filters"),
            }
            items.append(item)
            seen_chunks.add(cid)
        elif k == "plan":
            p = evt.get("plan") or {}
            if product and p.get("product") != product:
                continue
            if source and p.get("source") != source:
                continue
            items.append({
                "ts": evt.get("ts"),
                "kind": "plan",
                "plan_id": evt.get("plan_id"),
                "product": p.get("product"),
                "source": p.get("source"),
                "date": p.get("date"),
                "chunks": len(p.get("chunks") or []),
                "probe_meta": p.get("probe_meta"),
            })
        elif k == "partition":
            upd = evt.get("update") or {}
            pkey = evt.get("partition_key") or ""
            parts = pkey.split("/")
            prod_ = parts[0] if len(parts) > 0 else None
            src_ = parts[1] if len(parts) > 1 else None
            date_ = parts[2] if len(parts) > 2 else None
            if product and prod_ != product:
                continue
            if source and src_ != source:
                continue
            items.append({
                "ts": evt.get("ts"),
                "kind": "partition",
                "partition_key": pkey,
                "product": prod_, "source": src_, "date": date_,
                "status": upd.get("status"),
                "update": upd,
            })

        if len(items) >= limit:
            break

    return {"items": items, "total_scanned": total_scanned, "log_exists": True}
