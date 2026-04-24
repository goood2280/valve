"""Valve 사용자 매뉴얼 PDF 생성.

실행: python Valve/scripts/gen_manual_pdf.py
출력: Valve/docs/valve_manual.pdf

Windows 기본 한글 폰트(Malgun Gothic) + Consolas(mono) 사용.
Linux/Mac 에서 실행 시 폰트 경로만 바꾸면 동작."""
from __future__ import annotations

import os
import sys
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm, mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle,
)

ROOT = Path(__file__).resolve().parents[1]
OUT_PATH = ROOT / "docs" / "valve_manual.pdf"
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

# ── Font registration ─────────────────────────────────────────────
FONT_REG = "KR"
FONT_MONO = "KRMono"
if os.name == "nt":
    pdfmetrics.registerFont(TTFont(FONT_REG, r"C:\Windows\Fonts\malgun.ttf"))
    pdfmetrics.registerFont(TTFont(FONT_MONO, r"C:\Windows\Fonts\consola.ttf"))
else:
    # 최소 fallback — 사용자 환경에 맞춰 수정 가능
    for cand in ("/System/Library/Fonts/AppleSDGothicNeo.ttc",
                 "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
                 "/usr/share/fonts/truetype/nanum/NanumGothic.ttf"):
        if Path(cand).exists():
            pdfmetrics.registerFont(TTFont(FONT_REG, cand))
            break
    for cand in ("/System/Library/Fonts/Menlo.ttc",
                 "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"):
        if Path(cand).exists():
            pdfmetrics.registerFont(TTFont(FONT_MONO, cand))
            break

# ── Styles ──
ss = getSampleStyleSheet()
BRAND = colors.HexColor("#FF5E00")
MUTE = colors.HexColor("#737373")
BG = colors.HexColor("#fff5ec")


def mk(name, parent="BodyText", **kw):
    base = ss[parent].clone(name)
    for k, v in kw.items():
        setattr(base, k, v)
    return base


S = {
    "Cover": mk("Cover", fontName=FONT_REG, fontSize=36, leading=42,
                textColor=BRAND, spaceAfter=18, alignment=0),
    "CoverSub": mk("CoverSub", fontName=FONT_REG, fontSize=13, leading=18,
                    textColor=MUTE),
    "H1": mk("H1", fontName=FONT_REG, fontSize=18, leading=24,
              textColor=BRAND, spaceAfter=10, spaceBefore=16),
    "H2": mk("H2", fontName=FONT_REG, fontSize=13, leading=18,
              textColor=colors.HexColor("#1e293b"),
              spaceAfter=6, spaceBefore=10),
    "P": mk("P", fontName=FONT_REG, fontSize=10.5, leading=16),
    "Hint": mk("Hint", fontName=FONT_REG, fontSize=9, leading=13, textColor=MUTE),
    "Code": mk("Code", fontName=FONT_MONO, fontSize=9, leading=13,
               textColor=colors.HexColor("#334155"),
               backColor=BG, borderPadding=5, borderWidth=0,
               leftIndent=6, rightIndent=6, spaceAfter=8, spaceBefore=4),
    "Small": mk("Small", fontName=FONT_REG, fontSize=9, leading=12),
}

story = []

def P(text, style="P"): story.append(Paragraph(text, S[style]))
def H1(text): P(text, "H1")
def H2(text): P(text, "H2")
def Code(text):
    # XML escape
    safe = (text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))
    story.append(Paragraph(f"<font name='{FONT_MONO}'>{safe.replace(chr(10), '<br/>')}</font>", S["Code"]))
def Hint(text): P(text, "Hint")
def Br(h=6): story.append(Spacer(1, h))
def HR():
    story.append(Table([[""]], colWidths=[17.5 * cm], rowHeights=[0.3],
                       style=[("BACKGROUND", (0, 0), (-1, -1), BRAND)]))
    Br(6)
def Row(items, widths=None, header=False):
    widths = widths or [4 * cm] + [(17.5 - 4) * cm / (len(items[0]) - 1)] * (len(items[0]) - 1)
    tbl = Table(items, colWidths=widths, repeatRows=1 if header else 0)
    base_style = [
        ("FONTNAME", (0, 0), (-1, -1), FONT_REG),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("LEADING", (0, 0), (-1, -1), 12),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("BOX", (0, 0), (-1, -1), 0.4, colors.HexColor("#cbd5e1")),
        ("INNERGRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#e2e8f0")),
        ("ROWBACKGROUNDS", (0, 0), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]
    if header:
        base_style += [
            ("BACKGROUND", (0, 0), (-1, 0), BRAND),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTSIZE", (0, 0), (-1, 0), 10),
        ]
    tbl.setStyle(TableStyle(base_style))
    story.append(tbl)
    Br(8)

# ══════════════════════════════════════════════════════════════════
# Cover
# ══════════════════════════════════════════════════════════════════
P("valve<font color='#737373'>.</font>", "Cover")
P("DataLake 수도꼭지 — 사용자 매뉴얼", "CoverSub")
P("turn the valve · feed the flow", "CoverSub")
Br(20)
HR()
P("<b>이 문서</b>는 Valve 를 사내에 배포하고 운영·확장하려는 엔지니어를 위한 실무 매뉴얼입니다. "
  "17개 섹션에 걸쳐 설정 · 사용법 · 알림 · 에이전트 연동 · flow 와의 통합까지 다룹니다.", "P")
Br(10)
P("<b>페어 앱</b>: flow (github.com/goood2280/flow)", "Small")
P("<b>저장소</b>: github.com/goood2280/valve (private)", "Small")
P("<b>버전</b>: v0.1.1 (2026-04-24)", "Small")
story.append(PageBreak())

# ══════════════════════════════════════════════════════════════════
# 1. 개요
# ══════════════════════════════════════════════════════════════════
H1("1. Valve 는 무엇인가")
P("Valve 는 사내 DataLake API 에서 데이터를 꺼내 <b>parquet staging</b> + <b>S3 업로드</b> 까지 "
  "흘려 flow 앱에 공급하는 수도꼭지입니다. 사내 실무 환경의 3가지 고통을 해결합니다.")
Br(4)
P("<b>① 쿼리 함수 호출 불안정성.</b> HY000/ODBC 에러 5% · 5분 timeout · 동시 실행 제약 — "
  "Valve 는 retry · rate-limit · chunk 분할로 흡수합니다.")
P("<b>② 하루치 데이터가 큼.</b> probe 로 분포 먼저 스캔 → shard 로 나눠 병렬 추출 → "
  "timeout 시 자동 re-shard.")
P("<b>③ 여러 제품·여러 소스의 관리 피로.</b> products.yaml / source_types.yaml 로 "
  "제품·DB 유형·추출 컬럼을 편집 가능. 웹 UI 에서 한눈에.")
Br(6)
P("<b>핵심 설계 원칙</b>: Robust &gt; Smart. "
  "모든 실패는 기록되고, 알림은 3-채널로 fan-out 되며, 설정이 손상되면 "
  "직전 정상값으로 fallback — 서비스가 중단되지 않습니다.", "Hint")

# ══════════════════════════════════════════════════════════════════
# 2. Quick Start
# ══════════════════════════════════════════════════════════════════
H1("2. Quick Start (5분 안에 돌려보기)")
Br(2)
P("<b>1) 설치</b>")
Code("git clone https://github.com/goood2280/valve\n"
     "cd valve\n"
     "pip install -r requirements.txt")
P("<b>2) 실행</b>")
Code("python -m uvicorn app:app --host 0.0.0.0 --port 8090")
P("<b>3) 브라우저 열기</b>: http://localhost:8090")
P("<b>4) 모니터 탭</b>에서 [▶ 전체 실행] 눌러 mock 데이터로 PRODA/PRODB 가 "
  "FAB/INLINE/ET 로 뽑혀 staging 에 쌓이는지 확인.")
P("<b>5) smoke test</b>")
Code("python scripts/smoke_test.py http://127.0.0.1:8090\n"
     "# 41개 항목 <0.5s 로 통과해야 정상")
Br(6)
P("<b>기본은 mock 모드</b>. 실 사내 API 연결은 Settings › Lake API 에서 "
  "mode=real + module 문자열 지정.", "Hint")

# ══════════════════════════════════════════════════════════════════
# 3. 아키텍처
# ══════════════════════════════════════════════════════════════════
H1("3. 아키텍처 한눈에")
Code("[사내 DataLake API]\n"
     "      │  lake_api.py  retry · timeout · rate-limit\n"
     "      ▼\n"
     "[Planner]   probe(sample_window|projection|none) → shard 분석 → chunk plan\n"
     "      │\n"
     "      ▼\n"
     "[Executor]  병렬 chunk 실행 · 실패→reshard · staging 저장\n"
     "      │\n"
     "      ├── staging/{product}/{source}/date=YYYY-MM-DD/*.parquet\n"
     "      │\n"
     "      ▼\n"
     "[S3 Queue]  upload_mode = immediate | interval | manual\n"
     "      │\n"
     "      ▼\n"
     "s3://bucket/{prefix}/{source}/{product}/date=.../part-0.parquet\n"
     "      │\n"
     "      ▼\n"
     "[flow app]  SplitTable · Dashboard · ML · …")
P("상태는 <b>StateStore</b> (jobs.jsonl append-only) 에 기록되고 <b>SSE 스트림</b> 으로 "
  "Monitor UI 에 실시간 반영됩니다.", "Hint")

story.append(PageBreak())

# ══════════════════════════════════════════════════════════════════
# 4. Settings — Lake API
# ══════════════════════════════════════════════════════════════════
H1("4. Settings › 🔌 사내 Lake API")
P("실제 데이터를 가져오는 함수 호출을 어떻게 할지 정합니다.")
Row([
    ["필드", "의미 · 권장값"],
    ["mode", "mock (데모/개발) 또는 real. real 로 바꾸면 아래 module 로드."],
    ["module", "mycorp.datalake:query 형태. importlib 로 동적 로드."],
    ["user", "사내 query 함수의 user 인자."],
    ["api_key", "사내 API 인증 키. 있을 때만 query(..., api_key=...) kwarg 로 전달. "
                 "저장 시 **** 마스킹."],
    ["timeout_sec", "5분(300) 이하 권장. 기본 290."],
    ["min_interval_sec", "rate-limit — 두 호출 사이 최소 간격(초)."],
    ["max_concurrent", "동시 chunk 수. 1~5 권장."],
    ["retry.attempts", "실패 시 최대 재시도 횟수."],
    ["retry.backoff_sec", "각 재시도 전 지연. [10,30,120] 같은 리스트."],
    ["retryable_errors", "재시도 트리거 문자열. HY000, TimeoutError 등."],
], widths=[4.2 * cm, 13.3 * cm], header=True)

P("<b>실 모드 팁</b>: query 함수가 api_key kwarg 를 안 받아도 TypeError 감지 후 "
  "3-인자 폴백하므로 구버전 어댑터 그대로 호환됩니다.", "Hint")

# ══════════════════════════════════════════════════════════════════
# 5. Settings — S3 업로드
# ══════════════════════════════════════════════════════════════════
H1("5. Settings › ☁ S3 업로드")
P("staging 에 쌓인 parquet 을 S3 로 언제 어떻게 올릴지 정합니다. "
  "<b>upload_mode</b> 3종을 이해하는 게 핵심.")
Row([
    ["필드", "의미"],
    ["endpoint_url", "비우면 AWS S3. MinIO 는 http://host:9000 형태."],
    ["bucket / prefix / access_key / secret_key", "표준 boto3 파라미터. secret_key 는 저장 후 ****."],
    ["fake_local_path", "endpoint_url 비우고 이 값 있으면 <b>개발 모드</b> — 로컬 폴더에 S3 흉내 쓰기."],
    ["upload_mode", "immediate (기본) | interval | manual"],
    ["upload_interval_sec", "interval 모드에서만 의미. 기본 300초(5분). 최소 5초 강제."],
    ["retry_failed_sec", "업로드 실패 항목 재시도 간격. 기본 120초. 3회 실패 시 알람."],
], widths=[5.5 * cm, 12 * cm], header=True)

H2("업로드 모드 선택 가이드")
Row([
    ["모드", "언제 쓰나", "특징"],
    ["immediate", "일반 운영. 소규모~중규모.",
     "chunk 완료 시 바로 put_atomic. 지연 적음, 실패 즉시 status=upload_failed."],
    ["interval", "대량 chunk 집중 발생 구간 (초기 시딩 중).",
     "큐잉 후 주기로 flush. S3 부하 평탄화. 실패는 자동 retry."],
    ["manual", "엔지니어가 S3 비용/타이밍 직접 제어.",
     "큐잉만. /api/jobs/s3-flush 눌러야 실제 올라감. CI/cron 통합 쉬움."],
], widths=[3 * cm, 6.5 * cm, 8 * cm], header=True)

# ══════════════════════════════════════════════════════════════════
# 6. Settings — 스케줄 / 프로브
# ══════════════════════════════════════════════════════════════════
H1("6. Settings › 📅 스케줄 · 🔍 프로브")
H2("스케줄")
Row([
    ["필드", "의미"],
    ["backfill_days", "오늘 + 과거 N일을 backfill 창으로 사용. 3~5 권장. "
                      "제품별 override 는 제품 탭의 backfill 필드."],
    ["interval_hours", "자동 스케줄 주기 (v0.2 예정 — 현재 수동 /enqueue-all)."],
    ["force_overwrite", "기존 파티션을 덮어쓸지. true 면 재실행 시 replace."],
    ["tolerance_pct", "completeness 허용 오차 %. 0.5 = 실제/예상 차이 0.5% 이내면 OK."],
], widths=[4 * cm, 13.5 * cm], header=True)

H2("프로브 (분포 미리보기)")
Row([
    ["필드", "의미"],
    ["strategy",
     "sample_window — 1시간 샘플 조회로 24배 비례 추정.\n"
     "projection — 하루치 전체지만 shard 컬럼만 받아 정확 분포.\n"
     "none — probe 생략, 단일 chunk 로 바로 실행."],
    ["window_hours", "sample_window 샘플 범위. 1시간 기본."],
    ["cache_days", "같은 (제품,소스) probe 결과 N일 재사용. 기본 7."],
    ["adaptive_correction", "probe 추정이 틀렸을 때 chunk 실행 중 감지·재분할."],
    ["fallback_on_timeout", "probe 자체가 timeout 나면 none 으로 즉시 진행."],
], widths=[4.5 * cm, 13 * cm], header=True)

P("<b>probe 가 자주 실패하는 소스</b>는 제품 탭의 소스 설정에 <code>probe skip</code> 체크박스 ON — "
  "해당 소스만 probe 없이 단일 chunk 로 바로 실행.", "Hint")

story.append(PageBreak())

# ══════════════════════════════════════════════════════════════════
# 7. Settings — 알림
# ══════════════════════════════════════════════════════════════════
H1("7. Settings › 🔔 알림 (3-채널 fan-out)")
P("이상이 발생하면 <b>S3 업로드 · flow 앱 푸시 · 범용 webhook</b> 3채널로 동시 전송합니다. "
  "각 채널은 독립 on/off. rate-limit + dedupe 로 폭주 차단.")

Row([
    ["필드", "의미"],
    ["enabled", "마스터 스위치. 끄면 모든 채널 무시."],
    ["min_severity", "info &lt; warn &lt; error &lt; critical. 이 레벨 미만은 drop."],
    ["max_per_hour", "시간당 최대 알람 수 (sliding window). 0 이면 무제한."],
    ["dedupe_window_sec", "같은 (kind + chunk_id) 에 대해 이 시간 내 중복 억제. 0 이면 없음."],
    ["s3_enabled / s3_prefix",
     "S3 에 알람 JSON 누적. prefix 기본 valve-alerts. "
     "s3://bucket/valve-alerts/20260424T121314-chunk_failed.json 형태."],
    ["flow_enabled / flow_notify_url",
     "flow 앱 알림 엔드포인트. Valve↔flow 연동 기본 채널."],
    ["webhook_enabled / webhook_url",
     "범용 webhook. Slack/Teams 어댑터 등."],
    ["config_prefix",
     "Valve 기동 시 S3 에서 settings/products/source_types 를 pull 할 prefix. "
     "기본 valve-config."],
], widths=[5.5 * cm, 12 * cm], header=True)

H2("알람 이벤트 페이로드")
Code('{\n'
     '  "ts": 1776991200.0,\n'
     '  "source": "valve.executor",\n'
     '  "kind": "chunk_failed",\n'
     '  "severity": "error",\n'
     '  "title": "PRODA/FAB/2026-04-24 chunk failed",\n'
     '  "chunk_id": "PRODA-FAB-2026-04-24-00",\n'
     '  "product": "PRODA", "source": "FAB", "date": "2026-04-24",\n'
     '  "error_type": "HY000Error",\n'
     '  "error": "[HY000] ODBC driver error",\n'
     '  "dispatch": {\n'
     '    "s3": {"ok": true, "key": "valve-alerts/20260424T120000-chunk_failed.json"},\n'
     '    "flow": {"ok": true, "message": "200 "},\n'
     '    "webhook": null\n'
     '  }\n'
     '}')

P("GET <code>/api/alerts/recent?limit=50</code> 로 최근 200건 메모리 버퍼 조회 가능.", "Hint")

# ══════════════════════════════════════════════════════════════════
# 8. Settings — 소스 타입
# ══════════════════════════════════════════════════════════════════
H1("8. Settings › 🧩 소스 타입 (동적 레지스트리)")
P("신규 DB 유형이 생기면 여기서 한 줄 추가 → 즉시 Products 편집기 드롭다운에 노출.")
P("내장 6종: <b>FAB · INLINE · ET · QTIME · EDS · VM</b>. "
  "각 소스 마다 컬럼 풀 · 기본 shard · 가이드 힌트 · 강조 색상이 지정됩니다.")

Row([
    ["필드", "의미"],
    ["name", "대문자 유일 키. 파티션 폴더/S3 prefix 에 그대로 쓰임."],
    ["table_template", "기본 테이블명. {name} 토큰은 name 으로 치환."],
    ["columns", "이 소스에서 추출 가능한 컬럼 풀 (제품 편집기 드롭다운)."],
    ["default_shard", "신규 소스 추가 시 제안되는 기본 shard_hierarchy."],
    ["accent", "hint 박스와 히트맵 테두리 색상 (HEX)."],
    ["hint", "가이드 문구. ` ` 로 감싸면 inline code 렌더."],
], widths=[4 * cm, 13.5 * cm], header=True)

P("저장하면 <code>config/source_types.yaml</code> 로 기록되고 "
  "<code>/api/schedule/source-types</code> GET/POST 로도 프로그램 접근 가능.", "Hint")

story.append(PageBreak())

# ══════════════════════════════════════════════════════════════════
# 9. Products 탭
# ══════════════════════════════════════════════════════════════════
H1("9. Products 탭 — 제품 · DB · 필터의 모든 것")
P("좌측 제품 목록 · 우측 상세 split. 제품 클릭으로 포커스, 내부 소스 탭으로 drill-down. "
  "각 소스 마지막에 <b>최종 Python 호출 미리보기</b>.")

H2("제품 헤더")
Row([
    ["요소", "용도"],
    ["name 입력", "PRODA/PRODB 식 이름."],
    ["enabled 체크", "끄면 스케줄에서 제외."],
    ["priority", "낮을수록 우선. enqueue-all 순서 정렬."],
    ["backfill", "제품별 backfill 일수 override. 비우면 전역값. "
                  "신규 세팅 시 300·600 등 크게 → 시딩 끝나면 비우기."],
    ["🚀 초기 시딩", "이 제품만 backfill 전 기간 × 모든 소스 일괄 투입."],
    ["🗑 제품 삭제", "products.yaml 에서 제거. 저장 눌러야 실제 반영."],
], widths=[3.5 * cm, 14 * cm], header=True)

H2("⚙ 제품 공통 기본 (1급 필드)")
P("<b>process_id · line_id · product_code</b> 세 가지는 제품 고유 식별자이므로 "
  "별도 그리드로 승격. 쉼표로 여러 개 입력 시 자동으로 IN 연산자.")
Code('params_template = {\n'
     '  "process_id":  {"op": "in", "value": ["P4203", "P4204"]},\n'
     '  "line_id":     {"op": "eq", "value": "L01"},\n'
     '  "product_code":{"op": "eq", "value": "PRODA"},\n'
     '}')

H2("⧗ 추가 필터")
P("그 외 WHERE 조건. <b>like / notLike</b> 연산자 지원 (<code>%AA%</code> 패턴).")
Code('# cata 라는 컬럼에 \'%AA%\' 패턴 제외\nparams_template["cata"] = {"op": "notLike", "value": "%AA%"}')

H2("▤ 추출 소스 + 최종 호출 미리보기")
P("소스별 탭 (FAB/INLINE/ET/…) 에 ✕ 버튼으로 삭제. "
  "+ 버튼으로 추가 (SOURCE_NAMES 6종 우선, 6종 초과 시 NEW).")
P("각 소스 카드 하단의 <b>🔎 이 소스의 최종 호출 (Python)</b>:")
Code('df: pandas.DataFrame = query(\n'
     '    params={\n'
     '        "table": "RAW_FAB_DATA",\n'
     '        "dateFrom": "2026-04-24T00:00:00",\n'
     '        "dateTo":   "2026-04-25T00:00:00",\n'
     '        "process_id": {"op": "in", "value": ["P4203", "P4204"]},\n'
     '        "product_code": {"op": "eq", "value": "PRODA"},\n'
     '    },\n'
     '    custom_col=["lot_id", "wafer_id", "time", "item_id", "value"],\n'
     '    user="pipe-runner",\n'
     '    api_key="{settings.lake_api.api_key}",\n'
     ')')

story.append(PageBreak())

# ══════════════════════════════════════════════════════════════════
# 10. Monitor 히트맵
# ══════════════════════════════════════════════════════════════════
H1("10. Monitor 히트맵 읽는 법")
P("14일치 (기본) 의 (제품 × 소스 × 날짜) 상태를 한 화면에.")
Row([
    ["색", "상태"],
    ["녹색 (Success)", "정상 완료. 완전성·업로드 OK."],
    ["파랑 (Running)", "현재 chunk 실행 중. 펄스 애니메이션."],
    ["노랑 (Partial / Tolerance)", "일부 chunk 실패했거나 완전성 초과. 재실행 고려."],
    ["빨강 (Failed / Upload err)", "치명. Logs 탭에서 원인 확인."],
    ["회색 (Planned)", "스케줄은 있으나 아직 실행 안 됨."],
    ["투명 (Idle)", "해당 날짜 소스 추출 계획 없음."],
    ["대각선 빗금 (미추출)", "이 제품은 해당 소스를 아예 추출하지 않음 (products.yaml 기준)."],
], widths=[5 * cm, 12.5 * cm], header=True)

P("<b>제품 그룹 헤더</b> 에는 priority · backfill override · (추출 중 / 전체 소스 수) 가 요약. "
  "각 셀 클릭 시 해당 (제품,소스,날짜) 재실행 투입 확인 다이얼로그.", "Hint")

# ══════════════════════════════════════════════════════════════════
# 11. Logs 탭
# ══════════════════════════════════════════════════════════════════
H1("11. Logs 탭 — 언제 시도했고 왜 실패했는지")
P("각 chunk / plan / partition 의 마지막 상태 기록. "
  "제품 · 소스 · 상태 · 실패만 · 종류 (chunk/plan/partition/all) 필터. 15초 자동 새로고침.")

Row([
    ["행 색", "의미"],
    ["빨강 배경", "failed / timeout_reshard / completeness_failed / upload_failed."],
    ["노랑 배경", "plan 단계에서 probe 실패 → 단일 chunk fallback."],
    ["흰색", "정상."],
], widths=[3.5 * cm, 14 * cm], header=True)

P("백엔드 소스: <code>GET /api/jobs/history?limit=300&amp;failed_only=true&amp;kind=chunk</code>. "
  "jobs.jsonl 뒤에서부터 tail. 같은 chunk_id 의 최신 상태만 1회 반환.", "Hint")

# ══════════════════════════════════════════════════════════════════
# 12. Browser 탭
# ══════════════════════════════════════════════════════════════════
H1("12. Browser 탭 — staging/s3_local parquet 검사")
P("추출된 parquet 을 좌측 트리로 탐색, 선택한 파일을 polars SQLContext 로 "
  "실시간 필터링.")

P("상단 <b>📘 SQL 사용 가이드</b> 접기 패널에 10가지 예시 snippet (클릭 시 적용).")

Code('-- 규칙:\n'
     '-- 1. 테이블명은 항상 t.  SELECT * FROM t WHERE ...\n'
     "-- 2. FROM 생략 시 자동으로 SELECT * FROM t WHERE ... 로 감쌈\n"
     "-- 3. 문자열은 'single-quote' (backtick/double-quote 아님)\n"
     "-- 4. 최대 2000 행. 이상은 LIMIT 명시\n"
     "-- 5. 날짜 비교는 ISO 문자열 또는 CAST(... AS TIMESTAMP)\n"
     "\n"
     "-- 예:\n"
     "SELECT lot_id, wafer_id, time, value\n"
     "FROM t\n"
     "WHERE item_id = 'ITEM_042'\n"
     "ORDER BY time DESC\n"
     "LIMIT 100")

story.append(PageBreak())

# ══════════════════════════════════════════════════════════════════
# 13. S3 Config Sync + 4-단계 Fallback
# ══════════════════════════════════════════════════════════════════
H1("13. S3 Config Sync — 설정을 중앙에서")
P("여러 Valve 인스턴스를 같은 설정으로 돌리려면 S3 의 <code>valve-config/</code> 에 "
  "settings.json · products.yaml · source_types.yaml 을 올려두고 각 인스턴스가 "
  "기동 시 pull 하게 합니다.")

Code("s3://bucket/valve-config/settings.json\n"
     "s3://bucket/valve-config/products.yaml\n"
     "s3://bucket/valve-config/source_types.yaml")

H2("4-단계 Fallback 체인 (robust &gt; fast)")
Row([
    ["#", "상태", "동작"],
    ["1", "S3 정상 + 파서 통과", "로컬 파일 교체 + .last_good 백업. 알람 없음."],
    ["2", "S3 파일 파싱 실패", "<b>config_s3_invalid</b> 경고 발행, 로컬 파일로 fallback."],
    ["3", "S3 미도달 (네트워크/권한)", "<b>config_s3_unreachable</b> 경고 발행, 로컬 파일로 fallback."],
    ["4", "로컬 파일마저 손상", "<b>config_local_corrupt</b> 에러 발행, .last_good 로 복구."],
    ["5", "전부 사용 불가", "<b>config_missing</b> 에러 발행, 번들 기본값으로 시작."],
], widths=[1.2 * cm, 4.5 * cm, 12 * cm], header=True)

P("각 단계 전환은 3-채널 알람(S3·flow·webhook) 으로 동시 브로드캐스트. "
  "운영자는 채널 하나만 보고 있어도 인지 가능.", "Hint")

# ══════════════════════════════════════════════════════════════════
# 14. Agent 연동 (오케스트레이터)
# ══════════════════════════════════════════════════════════════════
H1("14. Agent 연동 — 오픈소스 LLM 친화 설계")
P("<code>/api/agent/*</code> 네임스페이스로 진단·제안·실행 3-tier 제공. "
  "LLM 은 '읽고 고르고 호출' 만 함 — 창작 금지.")

Row([
    ["엔드포인트", "역할"],
    ["GET /api/agent/diagnose", "현재 이상 목록 (chunk_failed · stuck · probe_error · partition_partial). "
                                  "서버가 규칙 기반으로 판정."],
    ["POST /api/agent/suggest-fix", "이상 1건을 받아 후보 fix 반환 — action · args · confidence · "
                                     "rationale · safety(LOW/MEDIUM/HIGH)."],
    ["POST /api/agent/apply-fix", "액션 실행. dry_run: true 기본 — 검증만. "
                                   "false 로 전환해야 실 적용."],
    ["GET /api/agent/actions", "화이트리스트 액션 카탈로그 (retry_chunk · reshard_source · "
                                "toggle_probe_skip 등 8종)."],
    ["GET /api/agent/audit", "모든 agent 호출 감사 로그."],
], widths=[6 * cm, 11.5 * cm], header=True)

H2("안전 장치 5중")
P("1. 화이트리스트 외 액션은 400 Bad Request.", "Small")
P("2. 인자 누락 시 필요 인자 목록 반환.", "Small")
P("3. dry_run 우선 — 검증 실패하면 real 차단.", "Small")
P("4. 60초 cooldown per (action + args).", "Small")
P("5. 동일 키에 3연속 실패하면 suspend — 관리자 해제 필요.", "Small")

P("자세한 설계는 <code>docs/agent_design.md</code> 참고.", "Hint")

story.append(PageBreak())

# ══════════════════════════════════════════════════════════════════
# 15. flow 와 연동
# ══════════════════════════════════════════════════════════════════
H1("15. flow 앱과 연동")
P("flow 는 Valve 가 쓴 parquet 을 그대로 읽어 SplitTable/Dashboard/ML 에 활용합니다. "
  "포맷·파티셔닝·스키마는 이미 일치 — prefix 1줄만 정렬하면 끝.")

Row([
    ["관점", "일치 여부"],
    ["parquet 포맷 · hive date= 파티셔닝", "✅"],
    ["FAB/INLINE long-format (lot_id·wafer_id·time·item_id·value)", "✅"],
    ["ET hive or flat", "✅ (flow 가 둘 다 지원)"],
    ["원자성 (쓰는 도중 절대 안 보임)", "✅"],
    ["폴더 규약 <b>FAB/...</b> vs <b>1.RAWDATA_DB_FAB/...</b>", "⚠ prefix 정렬 필요"],
], widths=[10 * cm, 7.5 * cm], header=True)

H2("연결 옵션")
P("<b>A. 공유 워크스페이스 직접 쓰기 (가장 단순)</b>")
Code('# settings.json\n'
     '"s3": {\n'
     '  "endpoint_url": "",\n'
     '  "bucket": "db",\n'
     '  "prefix": "",\n'
     '  "fake_local_path": "/config/work/sharedworkspace/DB"\n'
     '}\n'
     '# executor.py:196 — 한 줄 변경\n'
     's3_key = f"1.RAWDATA_DB_{plan.source}/{plan.product}/date={plan.date}/part-0.parquet"')
P("→ Valve 가 <code>/config/work/sharedworkspace/DB/db/1.RAWDATA_DB_FAB/PRODA/date=.../part-0.parquet</code> "
  "에 쓰고, flow 의 db_root 를 <code>/config/work/sharedworkspace/DB/db</code> 로 맞추면 바로 인식.")

P("<b>B. 실 S3 경유 (사내 프로덕션)</b> — Valve 가 s3 에 올리고, "
  "flow 의 s3_ingest 가 다운싱크해서 db_root 로 풀어놓는 방식. "
  "각 앱이 S3 를 공유하되 파일시스템은 분리.", "Hint")

H2("양방향 원인 체인 (에이전트 orchestrator 경유)")
Code('# Valve 가 추출 실패 → 알람 → flow 에 공지 노출\n'
     'executor.py failure → ops.dispatch_alert\n'
     '  → POST settings.alerts.flow_notify_url (예: http://flow/api/valve/alert)\n'
     '  → flow 의 내부 notice 생성기 호출 → 관리자 belltray 에 표시\n'
     '\n'
     '# flow 에서 "데이터 없음" 감지 → orchestrator 가 Valve 진단\n'
     'flow 이상 감지 → orchestrator → GET valve/api/agent/diagnose\n'
     '               → POST valve/api/agent/apply-fix (retry_chunk, dry_run=false)\n'
     '               → Valve 자동 복구 → flow 데이터 정상화')

# ══════════════════════════════════════════════════════════════════
# 16. 트러블슈팅
# ══════════════════════════════════════════════════════════════════
H1("16. 트러블슈팅 플레이북")
Row([
    ["증상", "원인 1순위", "해결"],
    ["모든 chunk 가 HY000 재시도 후 실패",
     "사내 API 키 미등록 또는 만료",
     "Settings › Lake API › api_key 갱신. Logs 탭에서 error 메시지 확인."],
    ["partition 이 항상 completeness_failed",
     "tolerance_pct 가 너무 타이트",
     "Settings › 스케줄 › tolerance_pct 를 0.5 → 1.0 로."],
    ["특정 소스만 반복 timeout_reshard",
     "probe 가 부정확 → chunk 가 과대",
     "제품 탭 → 해당 소스 → <b>probe skip</b> 체크. "
     "또는 shard_hierarchy 에 2단 shard 추가."],
    ["S3 업로드 실패만 쌓임",
     "네트워크 장애 or 권한",
     "Settings › S3 credentials · endpoint_url 확인. "
     "upload_mode=interval + retry_failed_sec 로 자동 재시도."],
    ["기동 시 <b>config_s3_unreachable</b> 알람",
     "S3 pull 실패 (정상 동작 — 로컬 fallback)",
     "첫 번째 기동이라 S3 에 아직 설정 파일 없는 경우. "
     "UI 에서 설정 저장 후 수동으로 S3 에 올리면 다음 기동부터 정상."],
    ["알람이 너무 자주 와서 피로",
     "rate_limit·dedupe 미설정",
     "Settings › 알림 › max_per_hour=60 · dedupe_window_sec=60 권장."],
    ["Valve 재기동 시 10만+ 플랜 로딩 느림",
     "jobs.jsonl 이 커짐",
     "자동 rotate (50MB 기본) 가 snapshot 라인 남겨 1줄 복원 — 정상 동작. "
     "로그 보존 추가 원하면 .1~.5 파일 별도 백업."],
], widths=[5 * cm, 5 * cm, 7.5 * cm], header=True)

story.append(PageBreak())

# ══════════════════════════════════════════════════════════════════
# 17. 자주 쓰는 API 레퍼런스
# ══════════════════════════════════════════════════════════════════
H1("17. API 레퍼런스 (자주 쓰는 것만)")

H2("Jobs")
Row([
    ["메소드", "엔드포인트", "역할"],
    ["GET", "/api/jobs/state", "현재 snapshot (plans · chunks · partitions)"],
    ["GET", "/api/jobs/stream", "SSE — 실시간 이벤트"],
    ["POST", "/api/jobs/enqueue", "단일 (제품,소스,날짜) 실행"],
    ["POST", "/api/jobs/enqueue-all", "backfill 창 전체 일괄"],
    ["POST", "/api/jobs/enqueue-product", "제품 단위 초기 시딩"],
    ["POST", "/api/jobs/retry-partition", "(제품,소스,날짜) 재실행"],
    ["POST", "/api/jobs/cancel", "chunk 취소"],
    ["POST", "/api/jobs/probe-invalidate", "probe 캐시 무효화"],
    ["GET", "/api/jobs/s3-pending", "S3 업로드 대기 큐 조회"],
    ["POST", "/api/jobs/s3-flush", "대기 큐 즉시 플러시"],
    ["GET", "/api/jobs/history", "실행 이력 (필터 가능)"],
], widths=[1.5 * cm, 6.5 * cm, 9.5 * cm], header=True)

H2("Schedule · Products · Source Types")
Row([
    ["메소드", "엔드포인트", "역할"],
    ["GET", "/api/schedule", "스케줄 items"],
    ["GET", "/api/schedule/products", "products 조회"],
    ["POST", "/api/schedule/products", "products 저장 (자동 마이그레이션)"],
    ["GET", "/api/schedule/source-types", "소스 타입 레지스트리"],
    ["POST", "/api/schedule/source-types", "소스 타입 저장"],
    ["GET", "/api/schedule/columns", "제품/소스 컬럼 풀"],
], widths=[1.5 * cm, 6.5 * cm, 9.5 * cm], header=True)

H2("Ops · Agent · Alerts")
Row([
    ["메소드", "엔드포인트", "역할"],
    ["GET", "/api/metrics", "JSON 메트릭 (chunk/partition status, p50/p95)"],
    ["GET", "/api/metrics/prom", "Prometheus text format"],
    ["POST", "/api/alerts/test", "webhook 연결 테스트"],
    ["GET", "/api/alerts/recent", "최근 200건 알람 버퍼"],
    ["GET", "/api/agent/diagnose", "현재 이상 목록"],
    ["POST", "/api/agent/suggest-fix", "룰 기반 fix 제안"],
    ["POST", "/api/agent/apply-fix", "액션 실행 (dry_run 우선)"],
    ["GET", "/api/agent/actions", "화이트리스트 액션 카탈로그"],
    ["GET", "/api/agent/audit", "agent 호출 감사 로그"],
], widths=[1.5 * cm, 6.5 * cm, 9.5 * cm], header=True)

Br(12)
HR()
P("<b>이 매뉴얼의 pdf 생성 스크립트</b>: <code>Valve/scripts/gen_manual_pdf.py</code>. "
  "섹션 변경 후 재생성해 docs/valve_manual.pdf 로 덮어쓰기.", "Hint")
P("문의·개선 제안: github.com/goood2280/valve/issues.", "Hint")

# ── Build ─────────────────────────────────────────────────────────
doc = SimpleDocTemplate(
    str(OUT_PATH), pagesize=A4,
    leftMargin=2 * cm, rightMargin=2 * cm,
    topMargin=1.8 * cm, bottomMargin=1.8 * cm,
    title="Valve 사용자 매뉴얼 v0.1.1",
    author="goood2280",
)
doc.build(story)
print(f"Generated: {OUT_PATH}  ({OUT_PATH.stat().st_size:,} bytes)")
