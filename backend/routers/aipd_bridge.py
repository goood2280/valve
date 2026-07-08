# -*- coding: utf-8 -*-
"""aipd 브리지 — Valve 쪽 조립부.

aipd 공통 패키지(setup_valve.py 가 함께 배포)를 Valve 웹에 연결한다:
  - 순환 상태 조회 (/api/aipd/status)
  - event + wide form 발행 데모 (② AI namespace 분할 발행)
  - fab 미매칭 스캔 → 추천 → 검토큐 제출 (Flow 가 승인하는 순환의 시작점)
  - 승인 항목 자동 반영 (백그라운드 폴러 — manifest HEAD 최적화)
  - daily 정기 리포트 발행 데모
  - /aipd 미니 콘솔 페이지

aipd 가 없으면 이 라우터는 available=false 만 응답하고 Valve 본체는 정상 동작.
"""

from __future__ import annotations

import threading

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter()

try:
    import aipd  # noqa: F401

    _AIPD = True
except ImportError:
    _AIPD = False

_poller_started = False
_poller_log: list = []


def _demo_blocked():
    """mock 전용(합성 데이터) 데모는 실환경에서 동작하지 않는다."""
    if not _AIPD:
        return {"ok": False, "reason": "aipd 없음"}
    from aipd.compat import is_local_mode

    if not is_local_mode():
        return {"ok": False, "reason": "mock 데모는 로컬 모드 전용 — "
                "실환경에서는 실제 파이프라인(Planner/Executor)이 발행합니다"}
    return None


def _start_poller():
    """승인 항목 자동 반영 폴러 — Flow 승인 → 수초 내 Valve 반영 (유기적 순환)."""
    global _poller_started
    if _poller_started or not _AIPD:
        return
    _poller_started = True

    def _loop():
        from aipd.core import cycle

        def handler(item):
            _poller_log.append(
                {"review_id": item.review_id, "kind": item.kind, "title": item.title}
            )
            return True  # 데모: 반영 완료 마킹 (실환경: ConfigSync pull / apply-fix)

        cycle.watch_approved(handler, interval_sec=10)

    threading.Thread(target=_loop, daemon=True, name="aipd-approved-poller").start()


@router.get("/api/aipd/status")
def aipd_status():
    if not _AIPD:
        return {"available": False, "reason": "aipd 패키지 없음"}
    _start_poller()
    from aipd.core import cycle
    from aipd.core.env import summary

    return {
        "available": True,
        "env": summary(),
        "cycle": _safe(lambda: cycle.check()),
        "applied_recent": _poller_log[-10:],
    }


@router.post("/api/aipd/demo/publish")
def demo_publish(product: str = "PRODA", date: str = "2026-07-04"):
    """stub bigdataquery → wide form 변환 → ① event + ② AI namespace 발행."""
    blocked = _demo_blocked()
    if blocked:
        return blocked
    import io

    from aipd.compat import bigdataquery, get_s3_client
    from aipd.core.s3 import S3Layout, publish_wide_form

    long_df = bigdataquery.getData(
        {"table_name": "f_et_test"}, custom_columns=None, user_name="valve-demo"
    )
    idx = [c for c in long_df.columns if c not in ("item_id", "et_value")]
    long_df["et_value"] = long_df["et_value"].astype(float)
    wide = long_df.pivot_table(index=idx, columns="item_id",
                               values="et_value", aggfunc="first").reset_index()
    wide.columns.name = None
    wide = wide.rename(columns={c: c if str(c).startswith("ET_") or c in idx else f"ET_{c}"
                                for c in wide.columns})

    s3, lay = get_s3_client(), S3Layout()
    # ① event (flow 규약 경로)
    buf = io.BytesIO()
    long_df.to_parquet(buf, index=False)
    s3.put_object(Bucket=lay.bucket, Key=lay.event_key("ET", product, date),
                  Body=buf.getvalue())
    # ② AI namespace — 열 prefix 분할 + domain 별 manifest
    manifests = publish_wide_form(s3, wide, product=product, date=date)
    return {"ok": True, "event_key": lay.event_key("ET", product, date),
            "ai_domains": sorted(manifests), "rows": len(wide)}


@router.post("/api/aipd/demo/scan")
def demo_scan(product: str = "PRODA"):
    """fab 미매칭 step 스캔 → 패턴 추천 → 검토큐 제출 (Flow 승인 대기)."""
    blocked = _demo_blocked()
    if blocked:
        return blocked
    import pandas as pd

    from aipd.ai import valve_scanner

    fab = pd.DataFrame({
        "step_id": ["CC942300", "CC955100", "CC955150", "MT100200", "XX777700"],
        "eqp_id": ["ETCH_01", "CVD_02", "CVD_03", "MET_CD_01", "IMP_01"],
        "eqp_model": ["E1", "C2", "C2", "MEA-500", "I1"],
    })
    matching = pd.DataFrame({
        "product": [product] * 3,
        "step_id": ["CC942300", "CC941100", "CC955200"],
        "step_desc": ["GATE_ETCH", "GATE_ETCH", "SPACER_CVD"],
    })
    review_id = valve_scanner.scan_and_propose(fab, matching, product=product)
    return {"ok": bool(review_id), "review_id": review_id,
            "hint": "Flow 의 /aipd 콘솔에서 승인하면 Valve 폴러가 자동 반영합니다"}


@router.post("/api/aipd/demo/daily-report")
def demo_daily_report():
    """daily 정기 리포트 발행 데모 (stub 데이터 집계 → db/reports/daily/)."""
    blocked = _demo_blocked()
    if blocked:
        return blocked
    from datetime import datetime, timedelta

    import numpy as np
    import pandas as pd

    from aipd.compat import get_s3_client
    from aipd.report.periodic_report import compute_daily, publish_report

    rng = np.random.default_rng()
    today = datetime.now().strftime("%Y-%m-%d")

    def _day(d, lot, shift=0.0):
        return pd.DataFrame({
            "date": d, "root_lot_id": lot, "wafer_id": range(1, 6),
            "ET_VTH_N": 0.5 + shift + rng.normal(0, 0.01, 5),
        })

    hist = pd.concat([_day((datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d"),
                           f"H{i}") for i in range(1, 11)])
    cur = _day(today, "T_NEW", shift=0.05)     # 의도적 shift → 이상 감지 데모
    result = compute_daily(cur, hist, today)
    publish_report(get_s3_client(), result)
    return {"ok": True, "period": today,
            "anomalies": result["anomalies"], "totals": result["totals"]}


@router.post("/api/aipd/poll-approved")
def poll_approved_now():
    """승인 항목 즉시 1패스 반영 (백그라운드 폴러와 별개 수동 트리거)."""
    if not _AIPD:
        return {"ok": False, "reason": "aipd 없음"}
    from aipd.core import cycle

    def handler(item):
        _poller_log.append({"review_id": item.review_id, "kind": item.kind,
                            "title": item.title})
        return True

    n = cycle.poll_approved(handler)
    return {"ok": True, "applied": n, "recent": _poller_log[-10:]}


def _safe(fn):
    try:
        return fn()
    except Exception as e:
        return {"error": str(e)}


# ================================================================= 미니 콘솔
@router.get("/aipd", response_class=HTMLResponse)
def console():
    return _PAGE


_PAGE = """<!doctype html><html><head><meta charset="utf-8"><title>Valve × aipd</title>
<style>
body{font-family:'Segoe UI',sans-serif;background:#16181d;color:#ddd;margin:0;padding:16px}
h1{color:#f90;font-size:20px} button{background:#f90;color:#111;border:0;border-radius:5px;
padding:8px 14px;margin:4px;font-weight:700;cursor:pointer} button:hover{background:#fb3}
pre{background:#14161a;border:1px solid #333;border-radius:6px;padding:10px;font-size:12px;
max-height:300px;overflow:auto;white-space:pre-wrap}
.row{display:flex;gap:14px;flex-wrap:wrap}.col{flex:1;min-width:340px}
h3{color:#fb0;font-size:14px;margin:14px 0 6px}.dim{color:#888;font-size:12px}
</style></head><body>
<h1>Valve × aipd 콘솔</h1>
<div class="dim">Valve → S3 발행과 검토큐 제출, 승인 자동 반영을 트리거/관찰합니다.
(<a href="/" style="color:#f90">Valve 본체</a>)</div>
<div class="row"><div class="col">
<h3>동작 트리거</h3>
<button onclick="run('demo/publish','POST')">① event + ② wide form 발행</button>
<button onclick="run('demo/scan','POST')">미매칭 스캔 → 검토큐 제출</button>
<button onclick="run('poll-approved','POST')">승인 항목 즉시 반영</button>
<button onclick="run('demo/daily-report','POST')">daily 리포트 발행</button>
<h3>실행 결과</h3><pre id="out">버튼을 눌러보세요</pre>
</div><div class="col">
<h3>순환 상태 <span class="dim">(3초 자동 갱신 · 승인 폴러 상시 동작)</span></h3>
<pre id="st"></pre>
</div></div>
<script>
async function run(p,m){
  const r=await fetch('/api/aipd/'+p,{method:m||'GET'});
  document.getElementById('out').textContent=JSON.stringify(await r.json(),null,2);
  st();
}
async function st(){
  const r=await fetch('/api/aipd/status');
  document.getElementById('st').textContent=JSON.stringify(await r.json(),null,2);
}
st(); setInterval(st,3000);
</script></body></html>"""
