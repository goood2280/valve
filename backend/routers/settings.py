"""settings 라우터 — config 웹 CRUD (secret 마스킹)."""
from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from fastapi import APIRouter, HTTPException

from backend.core import llm

router = APIRouter(prefix="/api/settings", tags=["settings"])

_root: Path = None
_settings: dict = None
_api = None
_s3 = None

# 마스킹 대상 = S3 자격증명 + 사내 LLM 토큰.
# 사내 Lake API 는 user_name 만 쓰므로 비밀값이 없다.
MASKED_KEYS = {"secret_key", "access_key", "token"}
_MASKED_SECTIONS = ("s3", "lake_api", "llm")


def deps(root: Path, settings: dict, api, s3):
    global _root, _settings, _api, _s3
    _root = root
    _settings = settings
    _api = api
    _s3 = s3


def _mask(cfg: dict) -> dict:
    out = deepcopy(cfg)
    for section in _MASKED_SECTIONS:
        cfg_sec = out.get(section, {})
        for k in MASKED_KEYS:
            if cfg_sec.get(k):
                cfg_sec[k] = "****"
        # 구버전 설치에 남아 있는 사용하지 않는 키 (사내 API 는 user_name 만 쓴다)
        cfg_sec.pop("api_key", None)
    return out


def _merge(base: dict, upd: dict):
    for k, v in upd.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _merge(base[k], v)
        elif k in MASKED_KEYS and v == "****":
            continue  # masked 그대로면 보존
        else:
            base[k] = v


@router.get("")
def get_settings():
    return _mask(_settings)


@router.post("")
def update_settings(req: dict):
    # Valve는 실 API 전용이다. 구버전 UI/설정에서 mode가 넘어와도 저장하지 않는다.
    req = deepcopy(req or {})
    if isinstance(req.get("lake_api"), dict):
        req["lake_api"].pop("mode", None)
        module = str(req["lake_api"].get("module") or "")
        if module and "mock" in module.lower():
            raise HTTPException(400, "Mock 어댑터는 지원하지 않습니다. 실 API module을 지정하세요.")
    _settings.setdefault("lake_api", {}).pop("mode", None)
    _merge(_settings, req)
    path = _root / "config" / "settings.json"
    path.write_text(
        json.dumps(_settings, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    # 런타임 반영
    try:
        _api.reload(_settings)
    except Exception:
        pass
    try:
        _s3.reload(_settings)
    except Exception:
        pass
    # 설정을 고친 직후는 사람이 개입한 시점이다 — LLM 차단기를 닫아 바로 다시 시도하게 한다
    llm.reset_health()
    return {"ok": True, "settings": _mask(_settings)}


@router.get("/ai")
def ai_status():
    """사내 LLM 연결 상태 (비밀값 없음). 프론트는 available 로 AI 버튼 노출만 정한다 —
    AI 가 없어도 화면은 규칙 요약으로 그대로 동작한다."""
    return llm.status()


@router.post("/ai/test")
def ai_test():
    """연결 테스트 — 짧은 프롬프트 한 번. 실패해도 500 이 아니라 ok=false 로 돌려준다
    (설정 화면에서 원인을 그대로 읽어야 하므로)."""
    return llm.probe()


@router.get("/schema")
def get_schema():
    """UI 폼 생성용 최소 스키마 힌트."""
    return {
        "lake_api": {
            "module": "str '패키지.모듈:함수'. 함수 시그니처는 "
                      "query(params, custom_col, user) — 사내 getData 는 keyword 형이라 "
                      "getData(params, custom_columns=custom_col, user_name=user) 로 넘기는 "
                      "얇은 어댑터를 만들고 그 경로를 적는다",
            "user": "str (사내 getData 의 user_name — 이 API 의 인증 정보는 이것뿐이다)",
            "timeout_sec": "int (<300)",
            "min_interval_sec": "float",
            "max_concurrent": "int (1~5 권장)",
            "retry.attempts": "int",
            "retry.backoff_sec": "list[int]",
            "retryable_errors": "list[str]",
        },
        "s3": {
            "endpoint_url": "str (비우면 AWS)",
            "bucket": "str",
            "prefix": "str",
            "access_key": "str (write-only)",
            "secret_key": "str (write-only)",
            "fake_local_path": "str (endpoint_url 비어있을 때 활성, 개발 모드)",
        },
        "schedule": {
            "backfill_days": "int (3~5 권장)",
            "interval_hours": "int (자동 스케줄, v0.2)",
            "force_overwrite": "bool",
            "tolerance_pct": "float (completeness 허용 %)",
        },
        "probe": {
            "strategy": ["sample_window", "projection", "none"],
            "window_hours": "float",
            "cache_days": "int (기본 7)",
            "adaptive_correction": "bool",
            "fallback_on_timeout": "bool",
        },
    }
