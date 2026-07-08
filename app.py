"""
Valve · app.py
--------------
FastAPI entry. 라우터 mount + dep injection + static frontend.

Valve — DataLake 의 수도꼭지. 사내 API 에서 데이터를 뽑아 S3 로 흘려 flow 에 공급.

실행:
    uvicorn app:app --host 0.0.0.0 --port 8090 --reload
"""
from __future__ import annotations

import json
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


# 테스트/임베디드 실행을 위해 VALVE_ROOT 환경변수로 ROOT 재지정 가능.
import os
ROOT = Path(os.environ.get("VALVE_ROOT") or Path(__file__).parent).resolve()
CONFIG_DIR = ROOT / "config"
LOGS_DIR = ROOT / "logs"
STAGING_DIR = ROOT / "staging"
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


def _migrate_params_template(products: dict) -> bool:
    """구 포맷 {slot: {column, op, value}} → 신 포맷 {column: {op, value}}.
    slot 명과 실제 컬럼명이 혼동되는 버그를 근본 해결. 변경 발생하면 True.
    """
    changed = False
    for p in products.get("products", []):
        tpl = p.get("params_template")
        if not isinstance(tpl, dict):
            continue
        new_tpl = {}
        for key, entry in tpl.items():
            if not isinstance(entry, dict):
                continue
            if "column" in entry:
                # old format: slot 을 무시하고 column 을 키로 승격
                col = entry.get("column") or key
                new_tpl[col] = {k: v for k, v in entry.items() if k != "column"}
                changed = True
            else:
                new_tpl[key] = entry
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

state = StateStore(LOGS_DIR / "jobs.jsonl")
planner = Planner(api, SETTINGS, PROBE_CACHE)
executor = ChunkExecutor(api, planner, s3, state, SETTINGS, STAGING_DIR)

# S3 업로드 큐 (immediate 모드면 enqueue 안 됨 — 단순 초기화만)
from backend.core import s3_queue as _s3queue
_s3queue.configure(s3, SETTINGS, state, LOGS_DIR / "s3_queue.jsonl", alert_cb=_boot_alert)


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
pipeline_router.deps(ROOT, SETTINGS, s3)

# browser: csv/설정파일(config) · 파이프라인 산출물(db) 탐색 + S3 연동 신호등.
# csv_sync(다운로드 상태) · s3(연동 여부) · s3_queue(업로드 대기) 를 근거로 판정.
from backend.core import s3_link  # noqa: E402

browser_router.deps(
    STAGING_DIR, S3_LOCAL_DIR,
    extra_roots={"config": CONFIG_DIR, "db": ROOT / "db"},
    annotator=s3_link.build_annotator(pipeline_router.csv_sync, s3, _s3queue),
    s3=s3, csv_sync=pipeline_router.csv_sync,
    config_prefix=(SETTINGS.get("alerts", {}).get("config_prefix") or "valve-config"),
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
    if (SETTINGS.get("s3") or {}).get("upload_mode") == "interval":
        _s3queue.start_background()
    # csv 설정파일 S3 주기 다운로드 (flow → Valve)
    if pipeline_router.csv_sync.load_config().get("enabled"):
        pipeline_router.csv_sync.start_background()
    # 파이프라인 주기 스케줄러 (전 vehicle raw→event→feature) — 항상 루프 기동,
    # 내부에서 runtime.schedule_enabled/interval_hours 를 폴링해 실제 실행 여부 결정.
    pipeline_router.runner.start_background()


@app.on_event("shutdown")
async def _on_shutdown():
    _s3queue.stop_background()
    pipeline_router.csv_sync.stop_background()
    pipeline_router.runner.stop_background()


@app.get("/api/health")
def health():
    return {
        "ok": True,
        "version": "0.1.0",
        "lake_mode": SETTINGS["lake_api"].get("mode"),
        "s3_fake": bool(SETTINGS["s3"].get("fake_local_path") and not SETTINGS["s3"].get("endpoint_url")),
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
if FRONTEND_DIR.exists() and any(FRONTEND_DIR.iterdir()):
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
