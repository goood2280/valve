"""사내 LLM 어댑터 + 진단 요약 — **없어도 돌아간다** 를 지키는지 검증.

이 기능의 계약은 하나다: AI 는 옵션이고, 꺼져 있거나 죽어 있어도
진단 요약은 규칙으로 나오며 예외는 절대 위로 올라가지 않는다.
"""
import pytest

from backend.core import diagnose_ai, llm


@pytest.fixture(autouse=True)
def clean_llm():
    llm.configure({})
    llm.reset_health()
    yield
    llm.configure({})
    llm.reset_health()


DIAG = {
    "vehicle": "VH_PRODA", "product": "PRODA", "blocked_at": "raw", "status": "fail",
    "stages": [
        {"key": "raw", "title": "1. raw 수집", "status": "fail", "checks": [
            {"name": "조회 모드", "status": "fail", "detail": "mock 합성 데이터",
             "fix": "설정 탭에서 어댑터를 지정하세요"},
            {"name": "FAB raw 파티션", "status": "ok", "detail": "12 행", "fix": ""},
        ]},
        {"key": "event", "title": "2. event 정확도", "status": "warn", "checks": [
            {"name": "FAB step 매칭률", "status": "warn", "detail": "50%", "fix": ""},
            {"name": "INLINE event", "status": "skip", "detail": "", "fix": ""},
        ]},
    ],
}


# ── 설정 정규화 ────────────────────────────────────────────
def test_defaults_are_off_and_unavailable():
    assert llm.is_available() is False
    assert llm.config()["enabled"] is False


def test_bad_values_fall_back_to_defaults():
    llm.configure({"llm": {"auth_mode": "천사", "format": "xml", "timeout_s": 9999,
                           "headers": "not a dict", "api_url": "  http://x/v1  "}})
    cfg = llm.config()
    assert cfg["auth_mode"] == "bearer"
    assert cfg["format"] == "openai"
    assert cfg["timeout_s"] == 180          # 상한으로 clamp
    assert cfg["headers"] == {}
    assert cfg["api_url"] == "http://x/v1"


def test_enabled_without_url_is_not_available():
    llm.configure({"llm": {"enabled": True}})
    assert llm.is_available() is False
    out = llm.complete("hi")
    assert out["ok"] is False and "api_url" in out["error"]


def test_status_never_leaks_token():
    llm.configure({"llm": {"enabled": True, "api_url": "https://llm.internal/v1",
                           "token": "SECRET-1234"}})
    st = llm.status()
    assert st["available"] is True and st["has_token"] is True
    assert "SECRET-1234" not in repr(st)


@pytest.mark.parametrize("url,expected", [
    ("https://llm.internal/v1", "https://llm.internal/v1/chat/completions"),
    ("https://llm.internal", "https://llm.internal/v1/chat/completions"),
    ("https://llm.internal/api/chat", "https://llm.internal/api/chat"),
])
def test_chat_url_accepts_base_or_full_path(url, expected):
    assert llm._chat_url(url, "openai") == expected


def test_raw_format_url_is_untouched():
    assert llm._chat_url("https://llm.internal/gen", "raw") == "https://llm.internal/gen"


# ── 호출 실패는 예외가 아니라 값 ────────────────────────────
def test_disabled_complete_returns_value_not_raise():
    out = llm.complete("hi")
    assert out["ok"] is False and out["text"] == ""


def test_failure_opens_breaker_and_probe_bypasses_it(monkeypatch):
    llm.configure({"llm": {"enabled": True, "api_url": "https://llm.internal/v1"}})
    calls = []

    def boom(*_a, **_k):
        calls.append(1)
        raise OSError("연결 거부")

    monkeypatch.setattr(llm.urllib.request, "urlopen", boom)
    assert llm.complete("hi")["ok"] is False
    assert len(calls) == 1

    # 차단기가 열린 동안은 나가지 않는다 (죽은 엔드포인트에 매번 timeout 을 기다리지 않게)
    second = llm.complete("hi")
    assert second["ok"] is False and len(calls) == 1
    assert llm.status()["breaker_open"] is True

    # 사람이 누른 연결 테스트는 다시 나간다
    llm.probe()
    assert len(calls) == 2


def test_successful_call_extracts_text(monkeypatch):
    llm.configure({"llm": {"enabled": True, "api_url": "https://llm.internal/v1",
                           "token": "t", "model": "gpt-oss-120b"}})
    sent = {}

    class FakeResp:
        def read(self, _n=None):
            return b'{"choices":[{"message":{"content":"  \xea\xb4\x9c\xec\xb0\xae\xec\x9d\x8c  "}}]}'

        def __enter__(self):
            return self

        def __exit__(self, *_a):
            return False

    def fake_urlopen(req, timeout=None):
        sent["url"] = req.full_url
        sent["headers"] = dict(req.headers)
        sent["body"] = req.data.decode("utf-8")
        return FakeResp()

    monkeypatch.setattr(llm.urllib.request, "urlopen", fake_urlopen)
    out = llm.complete("질문", system="시스템")
    assert out["ok"] is True and out["text"] == "괜찮음"
    assert sent["url"].endswith("/v1/chat/completions")
    assert sent["headers"]["Authorization"] == "Bearer t"
    assert '"role": "system"' in sent["body"]
    assert llm.status()["health"] == "healthy"


def test_dep_ticket_auth_mode_uses_its_own_header(monkeypatch):
    llm.configure({"llm": {"enabled": True, "api_url": "https://llm.internal/v1",
                           "token": "ticket", "auth_mode": "dep_ticket"}})
    hdrs = llm._headers(llm.config())
    assert hdrs["x-dep-ticket"] == "ticket" and "Authorization" not in hdrs


# ── 진단 요약 ──────────────────────────────────────────────
def test_summary_without_ai_falls_back_to_rules():
    out = diagnose_ai.summarize(DIAG)
    assert out["ok"] is True and out["source"] == "rules"
    assert "VH_PRODA" in out["text"] and "1. raw 수집" in out["text"]
    assert out["error"]          # 왜 규칙으로 갔는지 화면이 말해 줄 수 있어야 한다


def test_rule_summary_lists_fix_actions():
    text = diagnose_ai.rule_summary(DIAG)
    assert "할 일:" in text and "설정 탭에서 어댑터를 지정하세요" in text


def test_all_green_summary_is_not_alarming():
    green = {"vehicle": "VH", "product": "P", "blocked_at": None, "stages": [
        {"key": "raw", "title": "1. raw", "status": "ok",
         "checks": [{"name": "x", "status": "ok", "detail": "", "fix": ""}]}]}
    assert "전 단계 통과" in diagnose_ai.rule_summary(green)


def test_prompt_carries_only_problem_checks():
    prompt = diagnose_ai.build_prompt(DIAG)
    assert "조회 모드" in prompt and "FAB step 매칭률" in prompt
    assert "FAB raw 파티션" not in prompt      # ok 는 재료가 아니다
    assert "INLINE event" not in prompt        # skip 도 마찬가지
    assert "막힌 단계: 1. raw 수집" in prompt


def test_summary_uses_ai_when_connected(monkeypatch):
    monkeypatch.setattr(llm, "is_available", lambda: True)
    monkeypatch.setattr(llm, "complete", lambda *_a, **_k: {
        "ok": True, "text": "한 줄 요약: raw 가 mock 입니다.",
        "meta": {"model": "gpt-oss-120b", "latency_ms": 12}})
    out = diagnose_ai.summarize(DIAG)
    assert out["source"] == "ai" and out["model"] == "gpt-oss-120b"
    assert out["rule_text"]     # AI 를 못 믿을 때 대조할 규칙 요약도 같이 온다


def test_ai_failure_silently_degrades_to_rules(monkeypatch):
    monkeypatch.setattr(llm, "is_available", lambda: True)
    monkeypatch.setattr(llm, "complete", lambda *_a, **_k: {"ok": False, "text": "",
                                                           "error": "HTTP 503"})
    out = diagnose_ai.summarize(DIAG)
    assert out["source"] == "rules" and out["error"] == "HTTP 503"
    assert "VH_PRODA" in out["text"]


def test_empty_ai_response_is_treated_as_failure(monkeypatch):
    monkeypatch.setattr(llm, "is_available", lambda: True)
    monkeypatch.setattr(llm, "complete", lambda *_a, **_k: {"ok": True, "text": "   "})
    assert diagnose_ai.summarize(DIAG)["source"] == "rules"


# ── 설정 API ───────────────────────────────────────────────
def test_settings_api_masks_llm_token(app_client):
    r = app_client.post("/api/settings", json={
        "llm": {"enabled": True, "api_url": "https://llm.internal/v1", "token": "SECRET"}})
    assert r.status_code == 200
    assert r.json()["settings"]["llm"]["token"] == "****"

    # 마스킹된 값을 그대로 다시 저장해도 원본이 지워지지 않는다
    app_client.post("/api/settings", json={"llm": {"token": "****", "model": "m"}})
    st = app_client.get("/api/settings/ai").json()
    assert st["has_token"] is True and st["model"] == "m"


def test_ai_status_endpoint_reports_off_by_default(app_client):
    app_client.post("/api/settings", json={"llm": {"enabled": False, "api_url": ""}})
    st = app_client.get("/api/settings/ai").json()
    assert st["available"] is False and st["enabled"] is False
