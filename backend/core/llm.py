"""Valve · llm — 사내 LLM API **선택적** 어댑터.

flow `core/llm_adapter.py` 와 같은 정책을 Valve 크기로 줄인 것이다:

  · **AI 는 100% 옵션이다.** 설정이 없거나 꺼져 있거나 호출이 실패해도 Valve 는
    그대로 돈다. 진단 판정(ok/warn/fail)과 조치 문구는 전부 규칙 코드가 만들고,
    AI 는 그 위에 얹는 요약일 뿐이다 — 없으면 요약이 규칙 요약으로 내려앉는다.
  · **절대 예외를 올리지 않는다.** 실패는 `{"ok": False, "error": ...}` 로 돌려준다.
    파이프라인/진단 화면이 LLM 하나 때문에 죽으면 안 된다.
  · 사내 LLM 은 오픈소스 파인튜닝 수준이라 성능이 낮다 — 프롬프트는 짧게 쓰고,
    호출부는 항상 수동 fallback 을 남긴다.

설정은 `config/settings.json` 의 `llm` 블록 (S3 자격증명과 같은 파일이고,
settings 라우터가 `token` 을 마스킹한다). 설정 탭 › 🤖 AI 에서 편집한다.

    llm:
      enabled:   false            # 이 스위치 하나로 UI 버튼까지 사라진다
      api_url:   ""               # OpenAI 호환이면 ".../v1" 까지만 적어도 된다
      model:     "gpt-oss-120b"
      auth_mode: bearer|dep_ticket|none
      token:     ""               # bearer → Authorization, dep_ticket → x-dep-ticket
      headers:   {}               # 추가 헤더 (값 안의 {token} 은 치환된다)
      format:    openai|raw       # openai = messages[], raw = {"prompt": ...}
      extra_body: {}              # temperature 등 body 병합
      system_name/user_id/user_type: playground 계열 헤더 (값이 있을 때만 실린다)
      timeout_s: 20

한 번 실패하면 **차단기(circuit breaker)** 가 60초 열린다. 사내망에서 죽은
엔드포인트를 붙여 두면 화면마다 timeout 초를 그대로 기다리게 되기 때문이다.
"""
from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.request
import uuid
from urllib.parse import urlparse

DEFAULTS: dict = {
    "enabled": False,
    "api_url": "",
    "model": "gpt-oss-120b",
    "auth_mode": "bearer",
    "token": "",
    "headers": {},
    "format": "openai",
    "extra_body": {},
    "system_name": "",
    "user_id": "",
    "user_type": "",
    "timeout_s": 20,
}

_AUTH_MODES = ("bearer", "dep_ticket", "none")
_FORMATS = ("openai", "raw")
BREAKER_COOLDOWN_S = 60.0

_settings: dict = {}
_lock = threading.RLock()
_health: dict = {"status": "unknown", "until": 0.0, "error": "", "latency_ms": 0, "ok_at": 0.0}


def configure(settings: dict) -> None:
    """app.py 가 기동 때 SETTINGS 를 그대로 넘긴다 (dict 참조를 들고 있으므로
    설정 탭에서 저장하면 재기동 없이 반영된다 — settings 라우터가 같은 dict 를
    제자리 갱신한다)."""
    global _settings
    _settings = settings if isinstance(settings, dict) else {}


def config() -> dict:
    """정규화된 현재 설정. 잘못된 값은 조용히 기본값으로 되돌린다 —
    설정 오타 하나로 진단 화면이 500 이 되지 않게."""
    raw = (_settings.get("llm") if isinstance(_settings, dict) else None) or {}
    if not isinstance(raw, dict):
        raw = {}
    cfg = dict(DEFAULTS)
    cfg.update({k: v for k, v in raw.items() if k in DEFAULTS})
    cfg["enabled"] = bool(cfg["enabled"])
    for key in ("api_url", "model", "token", "system_name", "user_id", "user_type"):
        cfg[key] = str(cfg.get(key) or "").strip()
    cfg["auth_mode"] = str(cfg.get("auth_mode") or "bearer").strip().lower()
    if cfg["auth_mode"] not in _AUTH_MODES:
        cfg["auth_mode"] = "bearer"
    cfg["format"] = str(cfg.get("format") or "openai").strip().lower()
    if cfg["format"] not in _FORMATS:
        cfg["format"] = "openai"
    for key in ("headers", "extra_body"):
        if not isinstance(cfg.get(key), dict):
            cfg[key] = {}
    try:
        cfg["timeout_s"] = max(1, min(180, int(cfg.get("timeout_s") or 20)))
    except (TypeError, ValueError):
        cfg["timeout_s"] = 20
    return cfg


def is_available() -> bool:
    """켜져 있고 URL 이 있는가 — **연결 확인은 하지 않는다**.
    UI 는 이 값으로 AI 버튼 노출 여부만 정한다 (실패는 호출 시점에 드러난다)."""
    cfg = config()
    return bool(cfg["enabled"] and cfg["api_url"])


def status() -> dict:
    """설정 탭/진단 탭이 읽는 상태 — 비밀값은 담지 않는다."""
    cfg = config()
    with _lock:
        until = float(_health.get("until") or 0.0)
        snap = {
            "health": str(_health.get("status") or "unknown"),
            "last_error": str(_health.get("error") or ""),
            "last_latency_ms": int(_health.get("latency_ms") or 0),
            "breaker_open": time.time() < until,
            "cooldown_s": max(0, int(until - time.time())),
        }
    host = ""
    try:
        host = urlparse(cfg["api_url"]).hostname or ""
    except ValueError:
        host = ""
    snap.update({
        "available": is_available(),
        "enabled": cfg["enabled"],
        "configured": bool(cfg["api_url"]),
        "model": cfg["model"],
        "host": host,
        "auth_mode": cfg["auth_mode"],
        "has_token": bool(cfg["token"]),
        "timeout_s": cfg["timeout_s"],
    })
    return snap


# ── 차단기 ────────────────────────────────────────────────
def _mark_ok(latency_ms: int) -> None:
    with _lock:
        _health.update({"status": "healthy", "until": 0.0, "error": "",
                        "latency_ms": int(latency_ms), "ok_at": time.time()})


def _mark_fail(error: str, latency_ms: int = 0) -> None:
    with _lock:
        _health.update({"status": "unhealthy", "until": time.time() + BREAKER_COOLDOWN_S,
                        "error": str(error or "")[:240], "latency_ms": int(latency_ms)})


def reset_health() -> None:
    """차단기를 즉시 닫는다 (설정 저장·연결 테스트처럼 사람이 개입한 시점)."""
    with _lock:
        _health.update({"status": "unknown", "until": 0.0, "error": "", "latency_ms": 0})


def _breaker_open() -> bool:
    with _lock:
        return time.time() < float(_health.get("until") or 0.0)


# ── 요청 조립 ─────────────────────────────────────────────
def _chat_url(url: str, fmt: str) -> str:
    """`.../v1` 만 적어도 되게 — 사내 엔드포인트를 매번 전체 경로로 적게 하지 않는다."""
    clean = str(url or "").strip().rstrip("/")
    if fmt != "openai" or not clean:
        return str(url or "").strip()
    if clean.endswith("/v1"):
        return clean + "/chat/completions"
    if (urlparse(clean).path or "") in ("", "/"):
        return clean + "/v1/chat/completions"
    return str(url or "").strip()


def _headers(cfg: dict) -> dict:
    prompt_id, completion_id = str(uuid.uuid4()), str(uuid.uuid4())
    out = {"Accept": "application/json", "Content-Type": "application/json"}
    for k, v in (cfg.get("headers") or {}).items():
        if not k:
            continue
        out[str(k)] = (str(v).replace("{token}", cfg["token"])
                       .replace("{prompt_msg_id}", prompt_id)
                       .replace("{completion_msg_id}", completion_id))
    if cfg["token"]:
        if cfg["auth_mode"] == "bearer":
            out["Authorization"] = f"Bearer {cfg['token']}"
        elif cfg["auth_mode"] == "dep_ticket":
            out["x-dep-ticket"] = cfg["token"]
    # playground 계열은 헤더로 호출자를 식별한다 — 값이 있을 때만 싣는다
    for name, key in (("Send-System-Name", "system_name"), ("User-Id", "user_id"),
                      ("User-Type", "user_type")):
        if cfg.get(key):
            out[name] = cfg[key]
    if cfg.get("system_name"):
        out["Prompt-Msg-Id"], out["Completion-Msg-Id"] = prompt_id, completion_id
    return out


def _body(cfg: dict, prompt: str, system: str | None) -> dict:
    body = dict(cfg.get("extra_body") or {})
    if cfg["format"] == "openai":
        msgs = []
        if system:
            msgs.append({"role": "system", "content": system})
        msgs.append({"role": "user", "content": prompt})
        body["messages"] = msgs
    else:
        body["prompt"] = prompt
        if system:
            body["system"] = system
    if cfg["model"]:
        body["model"] = cfg["model"]
    return body


def _text_of(value) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "".join(_text_of(v.get("text") if isinstance(v, dict) else v) for v in value)
    return "" if value is None else str(value)


def extract_text(obj) -> str:
    """OpenAI/유사 응답에서 본문만. 모르는 스키마면 빈 문자열 —
    본문을 못 찾았을 때 dict 를 그대로 화면에 뿌리지 않는다."""
    if not isinstance(obj, dict):
        return _text_of(obj).strip()
    for choice in (obj.get("choices") or []):
        if not isinstance(choice, dict):
            continue
        msg = choice.get("message") or choice.get("delta") or {}
        text = _text_of(msg.get("content") if isinstance(msg, dict) else "")
        if not text:
            text = _text_of(choice.get("text"))
        if text.strip():
            return text.strip()
    for key in ("output_text", "text", "response", "content"):
        text = _text_of(obj.get(key))
        if text.strip():
            return text.strip()
    return ""


def complete(prompt: str, *, system: str | None = None, timeout: int | None = None,
             probe: bool = False) -> dict:
    """단일 프롬프트 완성. **예외를 올리지 않는다** — 항상 dict 를 돌려준다.

    probe=True 는 사람이 누른 연결 테스트다. 차단기를 무시하고 실제로 나가서
    "지금은 되는가" 를 다시 확인한다."""
    cfg = config()
    if not cfg["enabled"]:
        return {"ok": False, "text": "", "error": "AI 가 꺼져 있습니다 (설정 탭 › AI)"}
    if not cfg["api_url"]:
        return {"ok": False, "text": "", "error": "llm.api_url 이 비어 있습니다"}
    if not str(prompt or "").strip():
        return {"ok": False, "text": "", "error": "empty prompt"}
    if not probe and _breaker_open():
        with _lock:
            why = str(_health.get("error") or "최근 호출 실패")
        return {"ok": False, "text": "", "error": f"최근 실패로 잠시 건너뜁니다 — {why}"}

    to = int(timeout or cfg["timeout_s"])
    try:
        url = _chat_url(cfg["api_url"], cfg["format"])
        data = json.dumps(_body(cfg, prompt, system), ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers=_headers(cfg), method="POST")
    except Exception as e:                       # 설정 오타 — 여기서 끝낸다
        return {"ok": False, "text": "", "error": f"요청 조립 실패: {type(e).__name__}: {e}"}

    started = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=to) as resp:
            raw = resp.read(1024 * 1024).decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        detail = ""
        try:
            detail = e.read(400).decode("utf-8", errors="replace")
        except Exception:
            pass
        err = f"HTTP {e.code}: {detail}"[:240]
        _mark_fail(err, int((time.monotonic() - started) * 1000))
        return {"ok": False, "text": "", "error": err, "status_code": e.code}
    except Exception as e:
        err = f"{type(e).__name__}: {e}"[:240]
        _mark_fail(err, int((time.monotonic() - started) * 1000))
        return {"ok": False, "text": "", "error": err}

    latency = int((time.monotonic() - started) * 1000)
    _mark_ok(latency)
    try:
        text = extract_text(json.loads(raw))
    except Exception:
        text = raw.strip()
    if not text:
        return {"ok": False, "text": "", "error": "응답에서 본문을 찾지 못했습니다",
                "meta": {"model": cfg["model"], "latency_ms": latency}}
    return {"ok": True, "text": text,
            "meta": {"model": cfg["model"], "latency_ms": latency,
                     "prompt_chars": len(prompt), "response_chars": len(text)}}


def probe() -> dict:
    """설정 탭 '연결 테스트' — 짧은 프롬프트 한 번. 차단기는 먼저 닫고 시작한다."""
    reset_health()
    out = complete("ping. 한 단어로만 답하세요.", system="You are a health check.",
                   timeout=min(config()["timeout_s"], 15), probe=True)
    return {"ok": bool(out.get("ok")), "error": out.get("error", ""),
            "text": (out.get("text") or "")[:200], "status": status()}
