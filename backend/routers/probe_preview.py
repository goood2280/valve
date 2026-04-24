"""probe_preview 라우터 — dry-run probe + plan 미리보기 (업로드 없음)."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/api/probe-preview", tags=["probe"])

_planner = None
_products = None


def deps(planner, products):
    global _planner, _products
    _planner = planner
    _products = products


@router.post("")
async def probe_preview(req: dict):
    """req = {product, source, date}  → chunk plan 반환 (실행 X)."""
    prod = req.get("product"); src = req.get("source"); d = req.get("date")
    if not (prod and src and d):
        raise HTTPException(400, "product, source, date required")

    prod_cfg = next((p for p in _products["products"] if p["product"] == prod), None)
    if not prod_cfg:
        raise HTTPException(404, "product not found")
    src_cfg = next((s for s in prod_cfg.get("sources", []) if s["name"] == src), None)
    if not src_cfg:
        raise HTTPException(404, "source not found")

    plan = await _planner.build_plan(prod, src_cfg, prod_cfg, d)
    return {
        "plan_id": plan.plan_id,
        "chunks": [c.to_dict() for c in plan.chunks],
        "probe_meta": plan.probe_meta,
        "chunk_count": len(plan.chunks),
    }
