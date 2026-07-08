"""Valve smoke test - stdlib only (urllib / json), <5s, no external deps.

Usage:
  python Valve/scripts/smoke_test.py                    # default http://127.0.0.1:8090
  python Valve/scripts/smoke_test.py http://host:port

Exit code: 0 = all green, 1 = any failure.
"""
from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request

# Force UTF-8 on Windows console (ignore if already set)
try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

BASE = (sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8090").rstrip("/")

TIMEOUT = 8
passed = 0
failed = 0


def _req(method: str, path: str, body=None) -> tuple[int, dict | str]:
    url = BASE + path
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
            raw = r.read().decode("utf-8", errors="replace")
            try:
                return r.status, json.loads(raw)
            except Exception:
                return r.status, raw
    except urllib.error.HTTPError as e:
        try:
            raw = e.read().decode("utf-8", errors="replace")
        except Exception:
            raw = ""
        return e.code, raw
    except Exception as e:
        return 0, f"{type(e).__name__}: {e}"


def check(name: str, cond: bool, detail: str = ""):
    global passed, failed
    mark = "PASS" if cond else "FAIL"
    if cond:
        passed += 1
    else:
        failed += 1
    d = (detail or "")[:160]
    print(f"  [{mark}] {name}" + (f"  - {d}" if d else ""))


def section(title: str):
    print(f"\n== {title}")


def main():
    t0 = time.time()
    print(f"Valve smoke test @ {BASE}")

    # Health
    section("basics")
    st, d = _req("GET", "/api/health")
    check("GET /api/health 200", st == 200 and isinstance(d, dict), str(d)[:120])
    check("lake_mode present", isinstance(d, dict) and "lake_mode" in d, str(d.get("lake_mode") if isinstance(d, dict) else d))

    st, d = _req("GET", "/api/version")
    check("GET /api/version 200", st == 200 and isinstance(d, dict), str(d)[:120])

    st, d = _req("GET", "/")
    check("GET / (index.html) 200", st == 200, f"len={len(d) if isinstance(d, str) else '-'}")

    st, d = _req("GET", "/app.js")
    check("GET /app.js 200", st == 200, f"len={len(d) if isinstance(d, str) else '-'}")

    # Settings
    section("settings")
    st, d = _req("GET", "/api/settings")
    check("GET /api/settings 200", st == 200 and isinstance(d, dict))
    has_keys = isinstance(d, dict) and all(k in d for k in ("lake_api", "s3", "schedule", "probe"))
    check("settings has lake_api/s3/schedule/probe", has_keys)

    st, sch = _req("GET", "/api/settings/schema")
    check("GET /api/settings/schema 200", st == 200)

    # Schedule
    section("schedule · products · columns · source-types")
    st, d = _req("GET", "/api/schedule/products")
    check("GET /api/schedule/products 200", st == 200 and isinstance(d, dict))
    prods = d.get("products") if isinstance(d, dict) else []
    check("products list non-empty", isinstance(prods, list) and len(prods) > 0)

    st, d = _req("GET", "/api/schedule")
    check("GET /api/schedule 200", st == 200 and isinstance(d, dict))
    check("schedule has items/backfill_days/dates",
          isinstance(d, dict) and all(k in d for k in ("items", "backfill_days", "dates")))

    st, d = _req("GET", "/api/schedule/source-types")
    check("GET /api/schedule/source-types 200", st == 200 and isinstance(d, dict))
    stypes = d.get("source_types") if isinstance(d, dict) else []
    check("source_types list non-empty", isinstance(stypes, list) and len(stypes) > 0)
    # 기본 3종 모두 존재
    names = {(s.get("name") or "").upper() for s in (stypes or [])}
    for n in ("FAB", "INLINE", "VM"):
        check(f"source_type '{n}' present", n in names)

    # columns pool per source
    for src in ("FAB", "INLINE", "VM"):
        st, d = _req("GET", f"/api/schedule/columns?source={src}")
        ok = st == 200 and isinstance(d, dict) and isinstance(d.get("columns"), list) and len(d["columns"]) > 0
        check(f"/columns source={src} non-empty", ok, f"n={len(d.get('columns',[])) if isinstance(d, dict) else '-'}")

    # Jobs history + state
    section("jobs · state · history")
    st, d = _req("GET", "/api/jobs/state")
    check("GET /api/jobs/state 200", st == 200 and isinstance(d, dict))

    st, d = _req("GET", "/api/jobs/history?limit=5&kind=all")
    check("GET /api/jobs/history 200", st == 200 and isinstance(d, dict))
    check("history items list exists", isinstance(d, dict) and isinstance(d.get("items"), list))

    # Probe failure path: failed_only filter returns well-formed response even if 0 entries
    st, d = _req("GET", "/api/jobs/history?failed_only=true&limit=20")
    check("history failed_only 200", st == 200 and isinstance(d, dict))

    # Browser
    section("browser")
    st, d = _req("GET", "/api/browser/roots")
    check("GET /api/browser/roots 200", st == 200 and isinstance(d, dict))
    roots = (d.get("roots") if isinstance(d, dict) else []) or []
    check("browser has at least staging root",
          any((r.get("name") == "staging") for r in roots) if isinstance(roots, list) else False)

    # Ops (metrics + alerts)
    section("ops: metrics + alerts")
    st, d = _req("GET", "/api/metrics")
    check("GET /api/metrics 200", st == 200 and isinstance(d, dict))
    check("metrics has chunk_status", isinstance(d, dict) and "chunk_status" in d)
    st, d = _req("GET", "/api/metrics/prom")
    check("GET /api/metrics/prom 200", st == 200 and isinstance(d, str) and "valve_total_chunks" in d)

    # Agent scaffolding
    section("agent: diagnose + catalog + audit")
    st, d = _req("GET", "/api/agent/actions")
    check("GET /api/agent/actions 200", st == 200 and isinstance(d, dict))
    acts = {a["action"] for a in (d.get("actions") or [])} if isinstance(d, dict) else set()
    for name in ("retry_chunk", "retry_partition", "toggle_probe_skip",
                 "invalidate_probe_cache"):
        check(f"agent action '{name}'", name in acts)

    st, d = _req("GET", "/api/agent/diagnose")
    check("GET /api/agent/diagnose 200", st == 200 and isinstance(d, dict))

    st, d = _req("POST", "/api/agent/apply-fix",
                  body={"action": "not_allowed", "args": {}})
    check("agent unknown action → 400", st == 400)

    # Static frontend assets
    section("frontend static")
    st, d = _req("GET", "/style.css")
    check("GET /style.css 200", st == 200, f"len={len(d) if isinstance(d, str) else '-'}")
    st, d = _req("GET", "/index.html")
    check("GET /index.html 200", st == 200)

    # Summary
    total = passed + failed
    dt = time.time() - t0
    print(f"\n────────────")
    print(f"{passed}/{total} passed  ·  {failed} failed  ·  {dt:.2f}s")
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
