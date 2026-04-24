"""agent 라우터 — 오픈소스 LLM 수준 에이전트 연동용 안전 스캐폴딩.

docs/agent_design.md 참조. 핵심: 서버가 규칙 기반으로 진단·제안하고, 에이전트는
ID 기반 매칭 + 화이트리스트 액션만 호출. 모든 호출은 감사 로그.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/api/agent", tags=["agent"])

_state = None
_settings = None
_products = None
_planner = None
_executor = None
_audit_path: Path = None

# cooldown/rate-limit
_last_apply_by_key: dict[str, float] = {}
_APPLY_COOLDOWN_SEC = 60
_fail_counter: dict[str, int] = {}  # key -> consecutive fail count
_FAIL_SUSPEND_THRESHOLD = 3


ALLOWED_ACTIONS = {
    "retry_chunk":            {"args": ["chunk_id"],                 "safety": "LOW"},
    "retry_partition":        {"args": ["product", "source", "date"], "safety": "LOW"},
    "toggle_probe_skip":      {"args": ["product", "source", "value"], "safety": "LOW"},
    "invalidate_probe_cache": {"args": [],                            "safety": "LOW"},
    "reshard_source":         {"args": ["product", "source", "add_shard_key"], "safety": "MEDIUM"},
    "lower_backfill_override": {"args": ["product", "new_days"],      "safety": "MEDIUM"},
    "adjust_chunk_rows":      {"args": ["product", "source", "new_value"], "safety": "MEDIUM"},
    "enqueue_product_seed":   {"args": ["product"],                   "safety": "HIGH"},
}


def deps(state, settings, products, planner, executor, audit_path: Path):
    global _state, _settings, _products, _planner, _executor, _audit_path
    _state = state
    _settings = settings
    _products = products
    _planner = planner
    _executor = executor
    _audit_path = Path(audit_path)
    _audit_path.parent.mkdir(parents=True, exist_ok=True)


def _audit(endpoint: str, req: dict, result: dict, took_ms: int):
    try:
        evt = {
            "ts": time.time(), "endpoint": endpoint,
            "req": req, "result": result, "took_ms": took_ms,
        }
        with open(_audit_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(evt, ensure_ascii=False, default=str) + "\n")
    except Exception:
        pass


# ─────────────────── diagnose ───────────────────
@router.get("/diagnose")
def diagnose(limit: int = 50):
    """현재 이상 목록. 서버가 규칙 기반으로 판정. 에이전트는 읽기만."""
    t0 = time.time()
    snap = _state.snapshot() if _state else {"plans": {}, "chunks": {}, "partitions": {}}
    chunks = snap.get("chunks") or {}
    plans = snap.get("plans") or {}
    partitions = snap.get("partitions") or {}

    now = time.time()
    anomalies: list[dict] = []

    # chunk_failed
    for cid, c in chunks.items():
        st = c.get("status")
        if st in ("failed", "timeout_reshard", "upload_failed"):
            age = int(now - (c.get("ended_at") or c.get("started_at") or now))
            sev = "high" if age < 3600 else "medium"
            anomalies.append({
                "id": f"chunk-fail-{cid}",
                "kind": "chunk_failed",
                "severity": sev,
                "chunk_id": cid,
                "product": c.get("product"), "source": c.get("source"), "date": c.get("date"),
                "status": st,
                "error_type": c.get("error_type"), "error": c.get("error"),
                "age_sec": age,
                "tags": ["retryable"] if st != "upload_failed" else ["upload"],
            })
        elif st == "in_progress":
            started = c.get("started_at") or now
            age = int(now - started)
            if age > 1800:  # 30분 이상 in_progress
                anomalies.append({
                    "id": f"stuck-{cid}",
                    "kind": "stuck_in_progress",
                    "severity": "high" if age > 3600 else "medium",
                    "chunk_id": cid,
                    "product": c.get("product"), "source": c.get("source"), "date": c.get("date"),
                    "age_sec": age,
                    "tags": ["stuck"],
                })

    # probe_error
    for pid, plan in plans.items():
        meta = plan.get("probe_meta") or {}
        if meta.get("error"):
            anomalies.append({
                "id": f"probe-err-{pid}",
                "kind": "probe_error",
                "severity": "medium",
                "plan_id": pid,
                "product": plan.get("product"), "source": plan.get("source"),
                "date": plan.get("date"),
                "error": meta.get("error"),
                "tags": ["probe", "consider_probe_skip"],
            })

    # partition_partial
    for pkey, p in partitions.items():
        if p.get("status") == "partial_failed":
            anomalies.append({
                "id": f"part-part-{pkey}",
                "kind": "partition_partial",
                "severity": "medium",
                "partition_key": pkey,
                "product": p.get("product"), "source": p.get("source"), "date": p.get("date"),
                "tags": ["partial"],
            })

    # severity sort
    order = {"high": 0, "medium": 1, "low": 2}
    anomalies.sort(key=lambda x: (order.get(x.get("severity"), 3), -int(x.get("age_sec") or 0)))
    anomalies = anomalies[:limit]

    result = {"generated_at": now, "anomalies": anomalies, "count": len(anomalies)}
    _audit("/api/agent/diagnose", {"limit": limit}, {"count": result["count"]},
           int((time.time() - t0) * 1000))
    return result


# ─────────────────── suggest-fix ───────────────────
@router.post("/suggest-fix")
def suggest_fix(req: dict):
    t0 = time.time()
    anomaly = req.get("anomaly") or {}
    aid = req.get("anomaly_id") or anomaly.get("id")
    if not aid:
        raise HTTPException(400, "anomaly_id or anomaly required")

    # 만약 anomaly 전체가 오지 않았으면 diagnose 돌려 찾기
    if not anomaly:
        d = diagnose(limit=500)
        for a in d["anomalies"]:
            if a.get("id") == aid:
                anomaly = a
                break
        if not anomaly:
            raise HTTPException(404, f"anomaly {aid} not found")

    suggestions = _rule_based_suggestions(anomaly)
    result = {"anomaly_id": aid, "suggestions": suggestions, "count": len(suggestions)}
    _audit("/api/agent/suggest-fix", req, result, int((time.time() - t0) * 1000))
    return result


def _rule_based_suggestions(a: dict) -> list[dict]:
    k = a.get("kind")
    sugg: list[dict] = []
    if k == "chunk_failed":
        err_type = (a.get("error_type") or "").lower()
        cid = a.get("chunk_id")
        if cid:
            conf = 0.8 if "hy000" in err_type or "timeout" in err_type else 0.6
            sugg.append({"action": "retry_chunk", "args": {"chunk_id": cid},
                         "confidence": conf, "rationale": f"{err_type or 'failed'} 는 보통 재시도로 해결"})
            if "timeout" in err_type:
                sugg.append({"action": "reshard_source",
                             "args": {"product": a.get("product"), "source": a.get("source"),
                                      "add_shard_key": "lot_id"},
                             "confidence": 0.5,
                             "rationale": "timeout 은 chunk 가 크다는 신호 — shard 1단 추가"})
    elif k == "stuck_in_progress":
        if a.get("chunk_id"):
            sugg.append({"action": "retry_chunk", "args": {"chunk_id": a["chunk_id"]},
                         "confidence": 0.7,
                         "rationale": "30분+ in_progress — 대개 워커 hang 이므로 재시도"})
    elif k == "probe_error":
        sugg.append({"action": "toggle_probe_skip",
                     "args": {"product": a.get("product"), "source": a.get("source"), "value": True},
                     "confidence": 0.7,
                     "rationale": "probe 가 실패 → skip 으로 전환해 단일 chunk 로 진행"})
        sugg.append({"action": "invalidate_probe_cache",
                     "args": {"product": a.get("product"), "source": a.get("source")},
                     "confidence": 0.5,
                     "rationale": "캐시 무효화 후 다음 run 에서 재probe"})
    elif k == "partition_partial":
        sugg.append({"action": "retry_partition",
                     "args": {"product": a.get("product"), "source": a.get("source"),
                              "date": a.get("date")},
                     "confidence": 0.8,
                     "rationale": "partial_failed 는 해당 (제품,소스,날짜) 통째 재실행 추천"})
    # 에이전트가 선택할 때 참고할 action 메타
    for s in sugg:
        s["safety"] = ALLOWED_ACTIONS.get(s["action"], {}).get("safety", "UNKNOWN")
    return sugg


# ─────────────────── apply-fix ───────────────────
@router.post("/apply-fix")
async def apply_fix(req: dict):
    t0 = time.time()
    action = req.get("action")
    args = req.get("args") or {}
    dry_run = bool(req.get("dry_run", True))

    if action not in ALLOWED_ACTIONS:
        msg = {"ok": False, "error": "unknown action", "allowed": list(ALLOWED_ACTIONS.keys())}
        _audit("/api/agent/apply-fix", req, msg, int((time.time() - t0) * 1000))
        raise HTTPException(400, msg["error"])

    spec = ALLOWED_ACTIONS[action]
    for r in spec["args"]:
        if r not in args:
            msg = {"ok": False, "error": f"missing arg {r!r}", "required": spec["args"]}
            _audit("/api/agent/apply-fix", req, msg, int((time.time() - t0) * 1000))
            raise HTTPException(400, msg["error"])

    # cooldown (real apply only)
    key = f"{action}:{json.dumps(args, sort_keys=True, default=str)}"
    if not dry_run:
        last = _last_apply_by_key.get(key, 0.0)
        wait = _APPLY_COOLDOWN_SEC - (time.time() - last)
        if wait > 0:
            _audit("/api/agent/apply-fix", req, {"ok": False, "cooldown": int(wait)},
                   int((time.time() - t0) * 1000))
            raise HTTPException(429, f"cooldown {int(wait)}s")
        if _fail_counter.get(key, 0) >= _FAIL_SUSPEND_THRESHOLD:
            _audit("/api/agent/apply-fix", req, {"ok": False, "suspended": True},
                   int((time.time() - t0) * 1000))
            raise HTTPException(423, "action suspended — 사람 수동 해제 필요")

    # 실행
    try:
        result = await _dispatch(action, args, dry_run)
    except HTTPException:
        _fail_counter[key] = _fail_counter.get(key, 0) + 1
        raise
    except Exception as e:
        _fail_counter[key] = _fail_counter.get(key, 0) + 1
        result = {"ok": False, "error": f"{type(e).__name__}: {e}"}

    if result.get("ok") and not dry_run:
        _last_apply_by_key[key] = time.time()
        _fail_counter[key] = 0  # reset streak on success

    _audit("/api/agent/apply-fix",
           {"action": action, "args": args, "dry_run": dry_run}, result,
           int((time.time() - t0) * 1000))
    return result


async def _dispatch(action: str, args: dict, dry_run: bool) -> dict:
    import asyncio
    if action == "retry_chunk":
        cid = args["chunk_id"]
        snap = _state.snapshot()
        c = (snap.get("chunks") or {}).get(cid)
        if not c:
            return {"ok": False, "error": f"chunk {cid} not found"}
        prod = c.get("product"); src = c.get("source"); date = c.get("date")
        if not (prod and src and date):
            return {"ok": False, "error": "chunk metadata incomplete (product/source/date)"}
        if dry_run:
            return {"ok": True, "plan": f"retry chunk {cid} via {prod}/{src}/{date}"}
        prod_cfg = next((p for p in _products["products"] if p["product"] == prod), None)
        src_cfg = next((s for s in (prod_cfg or {}).get("sources", []) if s["name"] == src), None)
        if not (prod_cfg and src_cfg):
            return {"ok": False, "error": "product/source config not found"}
        plan = await _planner.build_plan(prod, src_cfg, prod_cfg, date)
        asyncio.create_task(_executor.run_plan(plan, prod_cfg, src_cfg))
        return {"ok": True, "plan_id": plan.plan_id}

    if action == "retry_partition":
        prod = args["product"]; src = args["source"]; date = args["date"]
        if dry_run:
            return {"ok": True, "plan": f"retry partition {prod}/{src}/{date}"}
        prod_cfg = next((p for p in _products["products"] if p["product"] == prod), None)
        src_cfg = next((s for s in (prod_cfg or {}).get("sources", []) if s["name"] == src), None)
        if not (prod_cfg and src_cfg):
            return {"ok": False, "error": "product/source config not found"}
        plan = await _planner.build_plan(prod, src_cfg, prod_cfg, date)
        asyncio.create_task(_executor.run_plan(plan, prod_cfg, src_cfg))
        return {"ok": True, "plan_id": plan.plan_id}

    if action == "toggle_probe_skip":
        prod = args["product"]; src = args["source"]; val = bool(args["value"])
        prod_cfg = next((p for p in _products["products"] if p["product"] == prod), None)
        src_cfg = next((s for s in (prod_cfg or {}).get("sources", []) if s["name"] == src), None)
        if not (prod_cfg and src_cfg):
            return {"ok": False, "error": "product/source not found"}
        if dry_run:
            return {"ok": True, "plan": f"set {prod}/{src}.probe_skip = {val}"}
        if val: src_cfg["probe_skip"] = True
        else: src_cfg.pop("probe_skip", None)
        _persist_products()
        return {"ok": True, "applied": {"product": prod, "source": src, "probe_skip": val}}

    if action == "invalidate_probe_cache":
        prod = args.get("product"); src = args.get("source")
        if dry_run:
            return {"ok": True, "plan": f"invalidate probe cache (product={prod}, source={src})"}
        _planner.invalidate(prod, src)
        return {"ok": True}

    if action == "reshard_source":
        prod = args["product"]; src = args["source"]; key = args["add_shard_key"]
        prod_cfg = next((p for p in _products["products"] if p["product"] == prod), None)
        src_cfg = next((s for s in (prod_cfg or {}).get("sources", []) if s["name"] == src), None)
        if not (prod_cfg and src_cfg):
            return {"ok": False, "error": "product/source not found"}
        cur = list(src_cfg.get("shard_hierarchy") or [])
        if key in cur:
            return {"ok": False, "error": f"{key} already in shard_hierarchy"}
        if dry_run:
            return {"ok": True, "plan": f"append {key} to shard_hierarchy: {cur} -> {cur+[key]}"}
        src_cfg["shard_hierarchy"] = cur + [key]
        _persist_products()
        return {"ok": True, "shard_hierarchy": src_cfg["shard_hierarchy"]}

    if action == "lower_backfill_override":
        prod = args["product"]; new_days = int(args["new_days"])
        prod_cfg = next((p for p in _products["products"] if p["product"] == prod), None)
        if not prod_cfg:
            return {"ok": False, "error": "product not found"}
        if dry_run:
            return {"ok": True, "plan": f"set {prod}.backfill_days_override = {new_days}"}
        if new_days <= 0:
            prod_cfg.pop("backfill_days_override", None)
        else:
            prod_cfg["backfill_days_override"] = new_days
        _persist_products()
        return {"ok": True, "backfill_days_override": prod_cfg.get("backfill_days_override")}

    if action == "adjust_chunk_rows":
        prod = args["product"]; src = args["source"]; new_val = int(args["new_value"])
        prod_cfg = next((p for p in _products["products"] if p["product"] == prod), None)
        src_cfg = next((s for s in (prod_cfg or {}).get("sources", []) if s["name"] == src), None)
        if not (prod_cfg and src_cfg):
            return {"ok": False, "error": "product/source not found"}
        if new_val < 10_000 or new_val > 5_000_000:
            return {"ok": False, "error": "new_value out of range [10_000, 5_000_000]"}
        if dry_run:
            return {"ok": True, "plan": f"set target_chunk_rows: {src_cfg.get('target_chunk_rows')} -> {new_val}"}
        src_cfg["target_chunk_rows"] = new_val
        _persist_products()
        return {"ok": True, "target_chunk_rows": new_val}

    if action == "enqueue_product_seed":
        prod = args["product"]
        prod_cfg = next((p for p in _products["products"] if p["product"] == prod), None)
        if not prod_cfg:
            return {"ok": False, "error": "product not found"}
        days = int(prod_cfg.get("backfill_days_override")
                   or (_settings.get("schedule") or {}).get("backfill_days") or 3)
        if dry_run:
            return {"ok": True, "plan": f"seed {prod} for {days} days × {len(prod_cfg.get('sources', []))} sources",
                    "requires_human_confirm": True}
        # HIGH safety 는 dry_run=false 인 경우에도 별도 확인 플래그 요구
        if not args.get("confirm_high_risk"):
            return {"ok": False, "error": "HIGH safety action — set args.confirm_high_risk=true to proceed"}
        from datetime import date, timedelta
        today = date.today()
        launched = 0
        for s in prod_cfg.get("sources", []):
            for i in range(days + 1):
                d = (today - timedelta(days=i)).isoformat()
                plan = await _planner.build_plan(prod, s, prod_cfg, d)
                asyncio.create_task(_executor.run_plan(plan, prod_cfg, s))
                launched += 1
        return {"ok": True, "launched": launched, "days": days}

    return {"ok": False, "error": f"dispatcher missing for {action}"}


def _persist_products():
    """products.yaml 을 디스크에 저장 (reshard/backfill/probe_skip/chunk_rows 변경 후)."""
    import yaml
    from pathlib import Path
    # ROOT 를 app.py 에서 전달받아야 하지만, 간단히 settings 에서 역추적
    root = Path(_settings.get("_root", Path(__file__).parents[2]))
    path = root / "config" / "products.yaml"
    try:
        path.write_text(
            yaml.safe_dump(_products, allow_unicode=True, sort_keys=False),
            encoding="utf-8",
        )
    except Exception:
        pass


@router.get("/audit")
def audit(limit: int = 100):
    """최근 감사 로그. 오케스트레이터가 에이전트 호출 history 확인용."""
    if not _audit_path or not _audit_path.exists():
        return {"items": [], "log_exists": False}
    limit = max(1, min(int(limit or 100), 5000))
    try:
        with open(_audit_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except Exception as e:
        raise HTTPException(500, f"audit read error: {e}")
    items = []
    for raw in reversed(lines):
        raw = raw.strip()
        if not raw: continue
        try: items.append(json.loads(raw))
        except Exception: continue
        if len(items) >= limit: break
    return {"items": items, "count": len(items), "log_exists": True}


@router.get("/actions")
def actions():
    """에이전트가 사용 가능한 액션 카탈로그 — bootstrap 에 사용."""
    return {
        "actions": [
            {"action": k, "args": v["args"], "safety": v["safety"]}
            for k, v in ALLOWED_ACTIONS.items()
        ]
    }
