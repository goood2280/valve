"""
Valve · app.py
--------------
FastAPI entry. 라우터 mount + dep injection + static frontend.

Valve — DataLake 의 수도꼭지. 사내 API 에서 데이터를 뽑아 S3 로 흘려 flow 에 공급.

실행:
    uvicorn app:app --host 0.0.0.0 --port 8090 --reload
"""
from __future__ import annotations

import sys

# backend/ 코드가 3.10+ 문법(list[str] | None)을 쓴다 — 낮은 버전이면 import 중
# 알 수 없는 TypeError 로 죽으므로 여기서 먼저 명확하게 알린다.
if sys.version_info < (3, 10):
    raise SystemExit(
        f"[valve] Python 3.10 이상이 필요합니다 — 현재 {sys.version.split()[0]}.\n"
        "        여러 버전이 설치돼 있다면:  py -3.11 -m uvicorn app:app --port 8090"
    )

import json
import mimetypes
from pathlib import Path

import yaml
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

# core
from backend.core.lake_api import LakeAPI
from backend.core.planner import Planner
from backend.core.s3_up import S3Uploader
from backend.core.state import StateStore
from backend.core.executor import ChunkExecutor

# routers
from backend.routers import jobs as jobs_router
from backend.routers import settings as settings_router
from backend.routers import schedule as schedule_router
from backend.routers import browser as browser_router
from backend.routers import query as query_router
from backend.routers import probe_preview as probe_preview_router
from backend.routers import ops as ops_router
from backend.routers import agent as agent_router
from backend.routers import pipeline as pipeline_router
from backend.routers import scanner as scanner_router
from backend.routers import s3_jobs as s3_jobs_router


# 테스트/임베디드 실행을 위해 VALVE_ROOT 환경변수로 ROOT 재지정 가능.
import os
ROOT = Path(os.environ.get("VALVE_ROOT") or Path(__file__).parent).resolve()
CONFIG_DIR = ROOT / "config"
LOGS_DIR = ROOT / "logs"
STAGING_DIR = ROOT / "staging"  # config 로드 후 db_root/0.STAGING 으로 재지정
S3_LOCAL_DIR = ROOT / "s3_local"
FRONTEND_DIR = ROOT / "frontend"
PROBE_CACHE = CONFIG_DIR / "probe_cache.json"

# ─── load config ───
_STARTUP_ALERTS: list[dict] = []


def _boot_alert(evt: dict):
    """startup 중에는 이벤트루프 전이라 바로 ops.dispatch 호출이 불가 → 버퍼링."""
    _STARTUP_ALERTS.append(evt)


# 1차: 로컬 settings/products 를 일단 읽음 (S3 부트스트랩에 필요)
SETTINGS = json.loads((CONFIG_DIR / "settings.json").read_text(encoding="utf-8"))
PRODUCTS = yaml.safe_load((CONFIG_DIR / "products.yaml").read_text(encoding="utf-8")) or {"products": []}


def _normalize_real_lake_settings(settings: dict) -> None:
    """구 설치/S3 설정의 mock mode·module을 실 API 기본값으로 마이그레이션."""
    lake = settings.setdefault("lake_api", {})
    lake.pop("mode", None)
    module = str(lake.get("module") or "").strip()
    if not module or "mock" in module.lower():
        lake["module"] = "backend.core.real_lake_adapter:query"


_normalize_real_lake_settings(SETTINGS)


def _migrate_params_template(products: dict) -> bool:
    """Normalize saved filters to the flat internal ``getData`` contract."""
    changed = False
    for p in products.get("products", []):
        tpl = p.get("params_template")
        if not isinstance(tpl, dict):
            continue
        new_tpl = {}
        for key, entry in tpl.items():
            col = entry.get("column") or key if isinstance(entry, dict) else key
            value = entry.get("value") if isinstance(entry, dict) else entry
            if col not in {"process_id", "line_id"}:
                changed = True
                continue
            if value is None or value == "" or value == []:
                changed = True
                continue
            new_tpl[col] = value
            if key != col or entry != value:
                changed = True
        p["params_template"] = new_tpl
    return changed


if _migrate_params_template(PRODUCTS):
    (CONFIG_DIR / "products.yaml").write_text(
        yaml.safe_dump(PRODUCTS, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )

# CWD 이슈 fix: fake_local_path 가 상대경로면 Valve ROOT 기준으로 절대화
_fl = (SETTINGS.get("s3") or {}).get("fake_local_path") or ""
if _fl and not Path(_fl).is_absolute():
    SETTINGS["s3"]["fake_local_path"] = str((ROOT / _fl).resolve())


# ─── init components ───
api = LakeAPI(SETTINGS)
s3 = S3Uploader(SETTINGS)

# ─── S3 config sync (기동 직후) ───────────────────────────────
# settings/products/source_types 를 S3 에서 pull. 실패 시 last_good fallback + 알람.
from backend.core.config_sync import ConfigSync  # noqa: E402
_cfg_sync = ConfigSync(
    s3_uploader=s3, root=CONFIG_DIR,
    s3_prefix=(SETTINGS.get("alerts", {}).get("config_prefix") or "valve-config"),
    alert_cb=_boot_alert,
)
_sync_result = {
    "settings": _cfg_sync.sync("settings.json", parser=json.loads, kind="json"),
    "products": _cfg_sync.sync("products.yaml", parser=yaml.safe_load, kind="yaml"),
    "source_types": _cfg_sync.sync("source_types.yaml", parser=yaml.safe_load, kind="yaml"),
}
# 동기화로 파일이 바뀌었으면 메모리 재로드
if _sync_result["settings"]["changed"]:
    SETTINGS = json.loads((CONFIG_DIR / "settings.json").read_text(encoding="utf-8"))
    _normalize_real_lake_settings(SETTINGS)
    # fake_local_path 절대화 재적용
    _fl2 = (SETTINGS.get("s3") or {}).get("fake_local_path") or ""
    if _fl2 and not Path(_fl2).is_absolute():
        SETTINGS["s3"]["fake_local_path"] = str((ROOT / _fl2).resolve())
    api.reload(SETTINGS); s3.reload(SETTINGS)
if _sync_result["products"]["changed"]:
    PRODUCTS.clear()
    PRODUCTS.update(yaml.safe_load((CONFIG_DIR / "products.yaml").read_text(encoding="utf-8")) or {"products": []})
    if _migrate_params_template(PRODUCTS):
        (CONFIG_DIR / "products.yaml").write_text(
            yaml.safe_dump(PRODUCTS, allow_unicode=True, sort_keys=False), encoding="utf-8")

# 파이프라인/직접 쿼리 산출물은 모두 같은 외부 DB 루트 아래에 둔다.
pipeline_router.deps(ROOT, SETTINGS, s3, api)
_DB_ROOT = pipeline_router._pipe.db_root()
STAGING_DIR = _DB_ROOT / "0.STAGING"

state = StateStore(LOGS_DIR / "jobs.jsonl")
_PRODUCT_VEHICLES: dict[str, list[str]] = {}
_PRODUCT_FILTERS: dict[str, dict] = {}
try:
    _VEHICLES = pipeline_router._pipe.vehicles()
except (FileNotFoundError, ValueError):
    _VEHICLES = {}
for _vehicle, _vcfg in _VEHICLES.items():
    _product = str(_vcfg.get("product") or _vehicle)
    _PRODUCT_VEHICLES.setdefault(_product, []).append(_vehicle)
    _filters = _PRODUCT_FILTERS.setdefault(_product, {"process_id": [], "line_id": []})
    for _key in ("process_id", "line_id"):
        _values = _vcfg.get(_key)
        _values = _values if isinstance(_values, list) else [_values]
        for _value in _values:
            if _value is not None and _value != "" and _value not in _filters[_key]:
                _filters[_key].append(_value)
for _filters in _PRODUCT_FILTERS.values():
    if len(_filters["process_id"]) == 1:
        _filters["process_id"] = _filters["process_id"][0]
planner = Planner(api, SETTINGS, PROBE_CACHE, product_filters=_PRODUCT_FILTERS)
executor = ChunkExecutor(api, planner, s3, state, SETTINGS, STAGING_DIR,
                         db_root=_DB_ROOT, product_vehicles=_PRODUCT_VEHICLES)
executor.product_filters = _PRODUCT_FILTERS

# S3 업로드 큐 (immediate 모드면 enqueue 안 됨 — 단순 초기화만)
from backend.core import s3_queue as _s3queue
_s3queue.configure(s3, SETTINGS, state, LOGS_DIR / "s3_queue.jsonl", alert_cb=_boot_alert)

# 사내 LLM — 있으면 진단 요약을 문장으로 받고, 없으면 규칙 요약으로 그대로 동작한다.
# SETTINGS dict 참조를 넘긴다: 설정 탭에서 저장하면 재기동 없이 반영된다.
from backend.core import llm as _llm
_llm.configure(SETTINGS)


# ─── FastAPI ───
app = FastAPI(title="Valve", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── router dep injection ───
jobs_router.deps(state, executor, planner, PRODUCTS, SETTINGS, LOGS_DIR / "jobs.jsonl")
settings_router.deps(ROOT, SETTINGS, api, s3)
schedule_router.deps(PRODUCTS, SETTINGS, ROOT)
probe_preview_router.deps(planner, PRODUCTS)
ops_router.deps(state, SETTINGS, s3)
SETTINGS["_root"] = str(ROOT)  # agent 가 products.yaml 경로 역추적할 때 사용
agent_router.deps(state, SETTINGS, PRODUCTS, planner, executor, LOGS_DIR / "agent_audit.jsonl")
scanner_router.deps(ROOT, SETTINGS, s3, pipeline_router._pipe, api)

# browser: csv/설정파일(config) · 파이프라인 산출물(db) 탐색 + S3 연동 신호등.
# csv_sync(다운로드 상태) · s3(연동 여부) · s3_queue(업로드 대기) 를 근거로 판정.
from backend.core import s3_link  # noqa: E402

# 알람 업로드 폴더 — 이 폴더 하나를 s3://{bucket}/{alerts.s3_prefix} 로 sync 하면
# flow 매칭알람이 갱신된다. 탐색기 root 는 sync 단위(= prefix 루트)로 노출.
_OUTBOX_SYNC_DIR = pipeline_router.alerts.outbox_sync_dir()
_OUTBOX_PREFIX = pipeline_router.alerts.prefix
if _OUTBOX_SYNC_DIR:
    _OUTBOX_SYNC_DIR.mkdir(parents=True, exist_ok=True)

browser_router.deps(
    STAGING_DIR, S3_LOCAL_DIR,
    extra_roots={"config": CONFIG_DIR, "db": _DB_ROOT,
                 **({"outbox": _OUTBOX_SYNC_DIR} if _OUTBOX_SYNC_DIR else {})},
    annotator=s3_link.build_annotator(pipeline_router.csv_sync, s3, _s3queue),
    s3=s3, csv_sync=pipeline_router.csv_sync,
    config_prefix=(SETTINGS.get("alerts", {}).get("config_prefix") or "valve-config"),
    outbox_prefix=_OUTBOX_PREFIX,
)

app.include_router(jobs_router.router)
app.include_router(settings_router.router)
app.include_router(schedule_router.router)
app.include_router(browser_router.router)
app.include_router(query_router.router)
app.include_router(probe_preview_router.router)
app.include_router(ops_router.router)
app.include_router(agent_router.router)
app.include_router(pipeline_router.router)
app.include_router(scanner_router.router)

# ─── S3 업/다운로드 항목 엔진 (탐색기 ⚙) ───────────────────────
# 로컬 경로 ↔ S3 key 를 짝지은 항목 단위로 수동 실행/중지·주기 실행.
# 기존 csv_sync.yaml(다운로드) · s3_transfer.yaml(업로드) 은 최초 1회 항목으로 이관.
from backend.core.s3_jobs import S3Jobs  # noqa: E402

s3jobs = S3Jobs(
    root=ROOT,
    uploader_for=browser_router._uploader_for,
    roots={"config": CONFIG_DIR, "staging": STAGING_DIR, "db": _DB_ROOT,
           **({"outbox": _OUTBOX_SYNC_DIR} if _OUTBOX_SYNC_DIR else {})},
    # 매칭 csv 를 새로 받으면 event/feature 재생성 — csv_sync 와 같은 훅
    on_downloaded=lambda paths: pipeline_router.on_config_downloaded(paths),
)
_migrated = s3jobs.migrate_if_empty(pipeline_router.csv_sync, browser_router.transfer_rules())
if _migrated:
    print(f"[valve] s3_jobs: 기존 설정에서 {_migrated}개 항목 이관 → config/s3_jobs.yaml")
# 매칭알람 폴더는 항상 폴더 단위 업로드 항목으로 — flow 가 읽는 valve-alerts/… 를
# db 폴더와 같은 전송 엔진(탐색기 ⚙)이 주기 sync 한다.
if s3jobs.ensure_outbox_item(_OUTBOX_PREFIX):
    print("[valve] s3_jobs: 매칭알람 outbox 업로드 항목 시드 (up_valve_alerts)")
_ml_products = [str(v.get("product") or name) for name, v in _VEHICLES.items()]
_ml_seeded = s3jobs.ensure_ml_table_items(_ml_products)
if _ml_seeded:
    print(f"[valve] s3_jobs: Flow ML_TABLE 업로드 항목 {_ml_seeded}개 시드")
s3_jobs_router.deps(s3jobs)
app.include_router(s3_jobs_router.router)
# 단계 정체 감시가 S3 전송까지 본다 — 로컬 단계가 다 초록이어도 업로드가 멈추면
# flow 는 옛 데이터를 계속 본다. AlertStore 는 s3jobs 보다 먼저 만들어지므로 여기서 꽂는다.
pipeline_router.attach_s3_jobs(s3jobs)

# aipd 브리지 (선택) — aipd 패키지가 함께 배포된 경우 순환 데모/검토큐 연동 활성화
try:
    from backend.routers import aipd_bridge as _aipd_bridge

    app.include_router(_aipd_bridge.router)
except Exception as _e:  # aipd 미설치 등 — Valve 본체는 정상 동작
    print(f"[valve] aipd bridge disabled: {_e}")


@app.on_event("startup")
async def _on_startup():
    """기동 중 버퍼된 config_sync 알람 발송 + S3 upload 모드가 interval 이면 백그라운드 루프 시작."""
    buffered = list(_STARTUP_ALERTS)
    _STARTUP_ALERTS.clear()
    for evt in buffered:
        try:
            await ops_router.dispatch_alert(evt)
        except Exception:
            pass
    await ops_router.flush_pending_alerts()
    if s3.is_configured() and (SETTINGS.get("s3") or {}).get("upload_mode") == "interval":
        _s3queue.start_background()
    # S3 항목 주기 실행 (업/다운로드) — 탐색기 ⚙ 에서 항목별 주기 설정
    s3jobs.start_background()
    # csv 설정파일 S3 주기 다운로드 (flow → Valve) — s3_jobs 로 이관됐으면 중복 실행 방지
    if (pipeline_router.csv_sync.load_config().get("enabled")
            and not any(i["direction"] == "download" for i in s3jobs.items())):
        pipeline_router.csv_sync.start_background()
    # 알람 S3 주기 발행 (Valve → flow) — 항상 루프 기동, 내부에서
    # alerts.s3_interval_min / s3_enabled 를 폴링해 실제 발행 여부 결정.
    pipeline_router.alerts.start_background()
    # 파이프라인 주기 스케줄러 (전 vehicle raw→event→feature) — 항상 루프 기동,
    # 내부에서 runtime.schedule_enabled/interval_hours 를 폴링해 실제 실행 여부 결정.
    pipeline_router.runner.start_background()


@app.on_event("shutdown")
async def _on_shutdown():
    _s3queue.stop_background()
    s3jobs.stop_background()
    pipeline_router.csv_sync.stop_background()
    pipeline_router.alerts.stop_background()
    pipeline_router.runner.stop_background()


@app.get("/api/health")
def health():
    return {
        "ok": True,
        "version": "0.1.0",
        "lake_mode": "real",
        "db_root": str(_DB_ROOT),
        "s3_fake": bool(s3.is_configured() and SETTINGS["s3"].get("fake_local_path")
                        and not SETTINGS["s3"].get("endpoint_url")),
        "s3_configured": s3.is_configured(),
        "staging": str(STAGING_DIR),
    }


_MODULE_DIR = Path(__file__).parent.resolve()

@app.get("/api/version")
def version():
    # VERSION.json 은 소스와 함께 배포됨 → ROOT(운영 데이터 디렉터리) 아닌 모듈 디렉터리에서 읽기
    try:
        return json.loads((_MODULE_DIR / "VERSION.json").read_text(encoding="utf-8"))
    except Exception:
        return {"name": "Valve", "version": "0.1.0"}


# ─── frontend static (v0.2 에서 index.html 추가 예정) ───
# Windows 는 레지스트리에서 MIME 을 읽는데 .woff2 가 등록돼 있지 않은 PC 가 많다 —
# 그러면 번들된 웹폰트가 text/plain 으로 나가고, CSP 나 프록시가 끼면 폰트가 통째로
# 안 뜬다 (사내망은 CDN 폴백도 없다). 등록되지 않은 채로 두지 않는다.
mimetypes.add_type("font/woff2", ".woff2")
mimetypes.add_type("font/woff", ".woff")

if FRONTEND_DIR.exists() and any(FRONTEND_DIR.iterdir()):
    # no-cache = 매 요청 재검증(304) — 업데이트 후 브라우저가 구버전 app.js 를
    # 계속 쓰는 문제 방지 (내용이 같으면 304 라 비용은 거의 없다)
    @app.middleware("http")
    async def _revalidate_static(request, call_next):
        resp = await call_next(request)
        p = request.url.path
        if p == "/" or p.endswith((".js", ".css", ".html")):
            resp.headers["Cache-Control"] = "no-cache"
        return resp

    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
else:
    @app.get("/")
    def root():
        return {
            "app": "Valve",
            "tagline": "turn the valve · feed the flow",
            "version": "0.1.0",
            "note": "frontend not yet built — v0.2. see /docs for API.",
            "health": "/api/health",
            "api_docs": "/docs",
        }


def main():
    """`valve` 콘솔 스크립트 — uvicorn 으로 앱 기동.
    VALVE_HOST/VALVE_PORT 로 조절 (기본 127.0.0.1:8090)."""
    import uvicorn

    uvicorn.run(
        app,
        host=os.environ.get("VALVE_HOST", "127.0.0.1"),
        port=int(os.environ.get("VALVE_PORT", "8090")),
    )


if __name__ == "__main__":
    main()
