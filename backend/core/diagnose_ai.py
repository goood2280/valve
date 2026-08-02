"""Valve · diagnose_ai — 진단 결과를 사람이 읽는 한 덩어리로 정리한다.

**판정은 여기서 만들지 않는다.** ok/warn/fail 과 조치 문구는 전부 `diagnose.py`
의 규칙 코드가 만든 것이고, 이 모듈은 그걸 *줄여서 옮겨 적을* 뿐이다. 그래서
두 경로가 같은 재료를 쓴다:

    rules  — 사내 LLM 이 없거나 꺼져 있거나 실패했을 때. 순수 코드, 항상 나온다.
    ai     — 설정 탭에서 AI 를 켜 두었을 때. 규칙 요약을 대체하는 게 아니라
             같은 사실을 문장으로 풀어 준다.

`summarize()` 는 **절대 실패하지 않는다** — AI 가 안 되면 조용히 규칙 요약으로
내려앉고 `error` 에 이유만 남긴다. 진단 화면은 AI 유무와 무관하게 동작해야 한다.
"""
from __future__ import annotations

from backend.core import llm

# 사내 LLM 은 오픈소스 파인튜닝 수준이라 길게 주면 오히려 헤맨다 — 재료를 줄여 준다.
MAX_CHECKS = 24
MAX_DETAIL = 200

SYSTEM = (
    "당신은 반도체 데이터 파이프라인(Valve) 운영을 돕는 도우미입니다. "
    "아래 '검사 결과'에 적힌 사실만 근거로 답하세요. "
    "검사 결과에 없는 원인·수치·파일명을 지어내지 마세요. "
    "한국어로, 군더더기 없이 짧게 씁니다."
)

TASK = (
    "다음 형식으로만 답하세요.\n"
    "한 줄 요약: (지금 무엇이 막혀 있는지 한 문장)\n"
    "원인 후보: (검사 결과가 가리키는 것만 1~3개, 각 한 줄)\n"
    "할 일: (사람이 순서대로 할 일 1~3개, 각 한 줄)\n"
)


def _problems(diag: dict) -> list[dict]:
    """확인이 필요한 검사만 (fail 먼저, 그다음 warn). skip/ok 는 재료가 아니다."""
    rows = []
    for stage in diag.get("stages") or []:
        for check in stage.get("checks") or []:
            if check.get("status") in ("fail", "warn"):
                rows.append({
                    "stage": stage.get("title") or stage.get("key") or "",
                    "status": check.get("status"),
                    "name": check.get("name") or "",
                    "detail": str(check.get("detail") or "")[:MAX_DETAIL],
                    "fix": str(check.get("fix") or "")[:MAX_DETAIL],
                })
    rows.sort(key=lambda r: 0 if r["status"] == "fail" else 1)
    return rows[:MAX_CHECKS]


def _blocked_title(diag: dict) -> str:
    key = diag.get("blocked_at")
    for stage in diag.get("stages") or []:
        if stage.get("key") == key:
            return str(stage.get("title") or key)
    return ""


def rule_summary(diag: dict) -> str:
    """AI 없이도 나오는 요약 — 규칙 판정을 순서대로 옮겨 적는다."""
    vehicle = diag.get("vehicle") or "-"
    rows = _problems(diag)
    fails = [r for r in rows if r["status"] == "fail"]
    warns = [r for r in rows if r["status"] == "warn"]
    blocked = _blocked_title(diag)

    if not rows:
        return f"한 줄 요약: {vehicle} — 확인이 필요한 검사가 없습니다 (전 단계 통과)."

    lines = []
    if blocked:
        lines.append(f"한 줄 요약: {vehicle} — 「{blocked}」 에서 막혔습니다 "
                     f"(실패 {len(fails)} · 확인 {len(warns)}).")
    else:
        lines.append(f"한 줄 요약: {vehicle} — 막힌 단계는 없지만 확인할 항목이 "
                     f"{len(warns)}건 있습니다.")

    lines.append("원인 후보:")
    for r in (fails or warns)[:3]:
        lines.append(f"- [{r['stage']}] {r['name']} — {r['detail'] or '상세 없음'}")

    todo = [r["fix"] for r in rows if r["fix"]]
    if todo:
        lines.append("할 일:")
        seen = []
        for fix in todo:
            if fix not in seen:
                seen.append(fix)
        for fix in seen[:3]:
            lines.append(f"- {fix}")
    return "\n".join(lines)


def build_prompt(diag: dict) -> str:
    """AI 에게 주는 재료 — 규칙 요약과 같은 사실만 담는다 (원본 표를 통째로 주지 않는다)."""
    rows = _problems(diag)
    head = [f"제품: {diag.get('vehicle')} (product {diag.get('product')})"]
    blocked = _blocked_title(diag)
    head.append(f"막힌 단계: {blocked}" if blocked else "막힌 단계: 없음")
    head.append("파이프라인 순서: raw → event → feature/ML_TABLE → SEND_FORM → S3 전송")

    lines = ["", "검사 결과 (fail = 여기서 막힘, warn = 진행되나 확인 필요):"]
    if not rows:
        lines.append("- 모든 검사 통과")
    for r in rows:
        lines.append(f"- [{r['status']}] ({r['stage']}) {r['name']}: {r['detail']}"
                     + (f" / 권장조치: {r['fix']}" if r["fix"] else ""))
    return "\n".join(head + lines + ["", TASK])


def summarize(diag: dict) -> dict:
    """진단 하나를 요약한다. AI 가 되면 ai, 아니면 rules — 어느 쪽이든 text 는 채워진다."""
    fallback = rule_summary(diag)
    if not llm.is_available():
        return {"ok": True, "source": "rules", "text": fallback, "model": "",
                "error": "AI 미설정 — 설정 탭 › 🤖 AI 에서 사내 LLM 을 연결하면 문장 요약을 받습니다."}
    out = llm.complete(build_prompt(diag), system=SYSTEM)
    if not out.get("ok") or not str(out.get("text") or "").strip():
        return {"ok": True, "source": "rules", "text": fallback, "model": "",
                "error": str(out.get("error") or "AI 응답이 비어 있습니다")}
    meta = out.get("meta") or {}
    return {"ok": True, "source": "ai", "text": str(out["text"]).strip(),
            "model": str(meta.get("model") or ""), "latency_ms": int(meta.get("latency_ms") or 0),
            "error": "", "rule_text": fallback}
