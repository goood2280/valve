"""schedule 라우터 — 예정 목록 (product/source × 최근 N일)."""
from __future__ import annotations

from copy import deepcopy
from datetime import date, timedelta
from pathlib import Path

import yaml
from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/api/schedule", tags=["schedule"])

_products: dict = None
_settings: dict = None
_root: Path = None


def deps(products: dict, settings: dict, root: Path):
    global _products, _settings, _root
    _products = products
    _settings = settings
    _root = root


def _products_file() -> Path:
    return _root / "config" / "products.yaml"


@router.get("")
def get_schedule():
    """rolling backfill 창 내의 모든 (제품·소스·날짜) 예정 목록.
    product.backfill_days_override 가 있으면 그 값으로 창을 늘림 (신규 세팅 시 600일 등)."""
    global_bf = int(_settings["schedule"].get("backfill_days", 3))
    today = date.today()
    max_bf = global_bf

    items = []
    for p in _products.get("products", []):
        if not p.get("enabled", True):
            continue
        p_bf = int(p.get("backfill_days_override") or 0) or global_bf
        max_bf = max(max_bf, p_bf)
        p_dates = [(today - timedelta(days=i)).isoformat() for i in range(p_bf + 1)]
        for s in p.get("sources", []):
            for d in p_dates:
                items.append({
                    "product": p["product"],
                    "source": s["name"],
                    "table": s.get("table"),
                    "date": d,
                    "priority": p.get("priority", 50),
                    "shard_hierarchy": s.get("shard_hierarchy", []),
                    "target_chunk_rows": s.get("target_chunk_rows"),
                    "backfill_days": p_bf,
                })
    items.sort(key=lambda x: (x["priority"], x["product"], x["source"], x["date"]),
               reverse=False)
    # 최신 날짜 우선 (같은 (product,source) 안)
    items.sort(key=lambda x: x["date"], reverse=True)
    dates = [(today - timedelta(days=i)).isoformat() for i in range(max_bf + 1)]
    return {"items": items, "backfill_days": global_bf, "max_backfill_days": max_bf, "dates": dates}


@router.get("/products")
def list_products():
    return _products


@router.post("/products")
def save_products(req: dict):
    """products.yaml 전체 replace — 웹 편집 저장."""
    if not isinstance(req, dict) or "products" not in req:
        raise HTTPException(400, "expected {products: [...]}")
    _products.clear()
    _products.update(req)
    _products_file().write_text(
        yaml.safe_dump(_products, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    return {"ok": True, "count": len(_products.get("products") or [])}


# source types 동적 레지스트리 — config/source_types.yaml. 신규 DB 추가도 웹/YAML 양쪽으로.
# 파일 없으면 built-in 6종 사용(아래 _BUILTIN_SOURCE_TYPES).
_BUILTIN_SOURCE_TYPES = [
    {"name": "FAB", "table_template": "RAW_{name}_DATA", "default_shard": [], "accent": "#64748b", "hint": "",
     "columns": ["lot_id", "wafer_id", "time", "eqp_id", "recipe_id", "step_id", "process_id", "line_id", "item_id", "value", "product_code"]},
    {"name": "INLINE", "table_template": "RAW_{name}_DATA", "default_shard": ["root_lot_id"], "accent": "#10b981",
     "hint": "INLINE 도 하루치가 크다 — `root_lot_id` probe 로 분포 스캔 후 shard 로 쪼개는 게 기본.",
     "columns": ["lot_id", "wafer_id", "root_lot_id", "time", "item_id", "value", "process_id", "line_id", "measure_pos", "product_code"]},
    {"name": "ET", "table_template": "RAW_{name}_DATA", "default_shard": ["root_lot_id", "item_id"], "accent": "#f59e0b",
     "hint": "ET 는 용량이 커서 보통 `item_id` 필터를 걸고, shard 는 `root_lot_id` 또는 `item_id` 로.",
     "columns": ["lot_id", "wafer_id", "root_lot_id", "item_id", "time", "value", "pattern_id", "die_x", "die_y", "process_id", "product_code"]},
    {"name": "QTIME", "table_template": "RAW_{name}_DATA", "default_shard": [], "accent": "#06b6d4",
     "hint": "QTIME 은 step 간 대기시간 — `from_step_id`·`to_step_id` 쌍으로 필터.",
     "columns": ["lot_id", "wafer_id", "step_id", "from_step_id", "to_step_id", "queue_start_time", "queue_end_time", "q_time_sec", "process_id", "line_id", "product_code"]},
    {"name": "EDS", "table_template": "RAW_{name}_DATA", "default_shard": [], "accent": "#8b5cf6",
     "hint": "EDS 는 die-level 전기특성 — `test_item` 필터 + `pattern_id` 기준 축소.",
     "columns": ["lot_id", "wafer_id", "die_x", "die_y", "pattern_id", "test_item", "value", "bin_code", "pass_fail", "process_id", "product_code"]},
    {"name": "VM", "table_template": "RAW_{name}_DATA", "default_shard": [], "accent": "#3b82f6",
     "hint": "VM — `recipe_id`·`step_id` 필터, `residual` 핵심 지표.",
     "columns": ["lot_id", "wafer_id", "eqp_id", "recipe_id", "step_id", "sensor_id", "predicted_value", "actual_value", "residual", "time", "process_id", "product_code"]},
]
_COMMON_COLUMNS = ["lot_id", "wafer_id", "time", "product_code"]


def _source_types_file() -> Path:
    return _root / "config" / "source_types.yaml"


def _load_source_types() -> list[dict]:
    fp = _source_types_file()
    if not fp.exists():
        return [dict(s) for s in _BUILTIN_SOURCE_TYPES]
    try:
        data = yaml.safe_load(fp.read_text(encoding="utf-8")) or {}
        items = data.get("source_types") or []
        if isinstance(items, list) and items:
            return items
    except Exception:
        pass
    return [dict(s) for s in _BUILTIN_SOURCE_TYPES]


def _source_types_by_name() -> dict[str, dict]:
    return {(s.get("name") or "").upper(): s for s in _load_source_types()}


@router.get("/source-types")
def source_types():
    """등록된 source type 전체 — FE 가 boot 시 1회 받아 SOURCE_NAMES·SOURCE_HINTS 구성."""
    return {"source_types": _load_source_types()}


@router.post("/source-types")
def save_source_types(req: dict):
    """source_types 전체 replace — 웹 편집 저장. 저장 후 즉시 /columns 에도 반영."""
    if not isinstance(req, dict) or "source_types" not in req:
        raise HTTPException(400, "expected {source_types: [...]}")
    items = req["source_types"]
    if not isinstance(items, list):
        raise HTTPException(400, "source_types must be a list")
    # 최소 검증: name 이 있고 unique
    seen = set()
    for it in items:
        nm = (it.get("name") or "").strip().upper()
        if not nm:
            raise HTTPException(400, "each source type requires a non-empty name")
        if nm in seen:
            raise HTTPException(400, f"duplicate source type: {nm}")
        seen.add(nm)
        it["name"] = nm
    _source_types_file().parent.mkdir(parents=True, exist_ok=True)
    _source_types_file().write_text(
        yaml.safe_dump({"source_types": items}, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    return {"ok": True, "count": len(items)}


@router.get("/columns")
def columns(product: str = "", source: str = ""):
    """지정 product/source 에 대해 사용 가능한 컬럼 풀을 반환.
    source_types.yaml 에 없는 source 도 지원 — product 에 저장된 custom_col 만 반환."""
    registry = _source_types_by_name()
    st = registry.get((source or "").upper())
    base = list(st.get("columns") if st else _COMMON_COLUMNS)
    # 현 product·source 에 이미 저장된 custom_col 은 풀에 합쳐 UI 누락 방지
    if product:
        for p in _products.get("products", []):
            if p.get("product") != product:
                continue
            for s in p.get("sources", []):
                if (s.get("name") or "").upper() == (source or "").upper():
                    for c in (s.get("custom_col") or []):
                        if c and c not in base:
                            base.append(c)
            for c in (p.get("custom_col") or []):
                if c and c not in base:
                    base.append(c)
    return {"columns": base, "source": source, "product": product}
