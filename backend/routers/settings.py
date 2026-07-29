"""settings 라우터 — config 웹 CRUD (secret 마스킹)."""
from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/api/settings", tags=["settings"])

_root: Path = None
_settings: dict = None
_api = None
_s3 = None

# 마스킹 대상 = S3 자격증명. 사내 Lake API 는 user_name 만 쓰므로 비밀값이 없다.
MASKED_KEYS = {"secret_key", "access_key"}


def deps(root: Path, settings: dict, api, s3):
    global _root, _settings, _api, _s3
    _root = root
    _settings = settings
    _api = api
    _s3 = s3


def _mask(cfg: dict) -> dict:
    out = deepcopy(cfg)
    for section in ("s3", "lake_api"):
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
    _merge(_settings, req or {})
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
    return {"ok": True, "settings": _mask(_settings)}


@router.get("/schema")
def get_schema():
    """UI 폼 생성용 최소 스키마 힌트."""
    return {
        "lake_api": {
            "mode": ["mock", "real"],
            "module": "str '패키지.모듈:함수' (real 모드에서만). 함수 시그니처는 "
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
