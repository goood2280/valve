"""
Valve · diagnose
----------------
"사내에 넣었더니 raw 부터 안 생긴다" 를 **단계별로 하나씩** 확인하는 진단.

    1. raw 가 되는가        — 조회가 실 API 로 나가는가, 파티션이 생겼는가,
                              조회 컬럼이 다 왔는가, 기준 열 날짜가 파티션과 맞는가
    2. event 가 정확한가    — 매칭 파일이 있는가, step_id 가 실제로 매칭되는가,
                              root_lot prefix 가 맞는가, step_desc 가 채워졌는가
    3. feature/매칭테이블   — 매칭 csv 들이 S3 에서 내려오고 있는가, 룰 대비 feature 가
                              다 나왔는가, ML_TABLE 이 flow 계약대로 발행됐는가
    4. SEND_FORM           — wide 를 prefix 그룹(KNOB/FAB+MASK/VM/INLINE)으로 쪼갠
                              최종 산출물이 wide 와 같은 컬럼·최신 시각으로 나왔는가
                              (전 제품 합산이라 vehicle 과 무관하다)
    5. S3 전송             — 만든 걸 실제로 계속 보내고 있는가. 앞 네 단계가 전부
                              초록이어도 여기가 멈추면 flow 는 옛 데이터를 본다

각 검사는 **그 자리에서 열어볼 parquet 을 같이 돌려준다** (`view`). 프론트 진단 탭이
그 값으로 탐색기(파일 뷰어)를 열기 때문에, "왜 실패인지" 를 화면에서 눈으로 확인할 수
있어야 한다는 것이 이 모듈의 설계 기준이다 — 숫자만 보여주고 끝내지 않는다.

  view = {"root": "db"|"config", "file": "<root 기준 상대경로>",
          "sql": "SELECT …", "label": "버튼에 쓸 문구"}

판정은 3단계: ok / warn(진행은 되지만 확인 필요) / fail(여기서 막혔다).
검사 자체가 터져도 뒤 단계는 계속 돈다 — 첫 실패에서 멈추면 "무엇까지 되는지" 를 못 본다.
"""
from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path

import polars as pl

from backend.core.feature_pipeline import SEND_GROUPS, WIDE_KEY, safe_filename

OK, WARN, FAIL, SKIP = "ok", "warn", "fail", "skip"
_RANK = {OK: 0, SKIP: 0, WARN: 1, FAIL: 2}

# 진단이 읽어들일 최대 행 — 사내 raw 는 하루치도 크다. 눈으로 확인할 표본만 본다.
SAMPLE_ROWS = 50_000


def _worst(statuses) -> str:
    out = OK
    for s in statuses:
        if _RANK.get(s, 0) > _RANK.get(out, 0):
            out = s
    return out


def _check(name: str, status: str, detail: str = "", view: dict | None = None,
           fix: str = "") -> dict:
    return {"name": name, "status": status, "detail": detail, "view": view, "fix": fix}


def _rel(pipe, path: Path) -> str | None:
    """db 루트 기준 상대경로 (탐색기 root='db' 에서 여는 값)."""
    try:
        return Path(path).resolve().relative_to(pipe.db_root().resolve()).as_posix()
    except (ValueError, OSError):
        return None


def _view(pipe, path: Path, sql: str = "SELECT * FROM t LIMIT 200",
          label: str = "parquet 열기") -> dict | None:
    rel = _rel(pipe, path)
    if rel is None:
        return None
    return {"root": "db", "file": rel, "sql": sql, "label": label}


def _config_view(pipe, path: Path, label: str) -> dict | None:
    try:
        rel = Path(path).resolve().relative_to((pipe.root / "config").resolve()).as_posix()
    except (ValueError, OSError):
        return None
    return {"root": "config", "file": rel, "sql": "", "label": label}


def _latest_partition(root: Path) -> Path | None:
    """가장 최근 date= 파티션의 parquet 하나 (없으면 None)."""
    if not root.is_dir():
        return None
    for d in sorted(root.glob("date=*"), reverse=True):
        files = sorted(d.glob("*.parquet"))
        if files:
            return files[-1]
    return None


def _read_head(path: Path, n: int = SAMPLE_ROWS) -> pl.DataFrame | None:
    """표본만 읽는다 — 진단은 조회 경로다. 사내 raw 는 하루치도 크고, 이 화면을
    열 때마다 파티션을 통째로 메모리에 올리면 파이프라인 쪽 워커까지 같이 죽는다."""
    try:
        return pl.scan_parquet(path).head(n).collect()
    except Exception:
        try:                      # scan 이 안 되는 구버전 파일은 직접 읽어 본다
            return pl.read_parquet(path).head(n)
        except Exception:
            return None


def _row_count(path: Path) -> int | None:
    """실제 행 수 (표본이 아니라) — parquet 메타만 읽는다."""
    try:
        return int(pl.scan_parquet(path).select(pl.len()).collect().item())
    except Exception:
        return None


def _stage(key: str, title: str, desc: str, checks: list[dict]) -> dict:
    real = [c for c in checks if c["status"] != SKIP]
    return {
        "key": key, "title": title, "desc": desc, "checks": checks,
        "status": _worst(c["status"] for c in checks) if real else SKIP,
        "failed": sum(1 for c in checks if c["status"] == FAIL),
        "warned": sum(1 for c in checks if c["status"] == WARN),
    }


# ══════════════════════════════════════════════════════════
# 1) raw — 사내 API 에서 데이터가 실제로 내려오는가
# ══════════════════════════════════════════════════════════
def _diag_raw(pipe, vehicle: str) -> dict:
    checks: list[dict] = []
    settings = getattr(pipe, "settings", None) or {}
    module = str(((settings.get("lake_api") or {}).get("module") or "")).strip()

    if pipe.lake_api is None:
        checks.append(_check(
            "조회 모드", FAIL,
            "lake_api 가 연결되지 않아 raw 가 **mock 합성 데이터**로 만들어집니다. "
            "이 상태의 산출물은 그대로 flow 로 나갑니다.",
            fix="설정 탭 → Lake API → module 에 사내 어댑터 경로를 넣고 재기동하세요."))
    elif "mock" in module.lower():
        checks.append(_check("조회 모드", WARN, f"lake_api.module = {module}",
                             fix="사내에서는 실 어댑터 경로여야 합니다."))
    else:
        checks.append(_check("조회 모드", OK,
                             f"실 어댑터 연결됨 — {module or '(기본)'} · "
                             f"user={(settings.get('lake_api') or {}).get('user') or '-'}"))

    for source, sc in pipe.sources_cfg().items():
        items = None
        try:
            items = pipe.reformatter_items(vehicle, source)
        except Exception:
            pass
        if items == []:
            checks.append(_check(f"{source} raw", SKIP,
                                 "이 제품의 reformatter 에 REAL 항목이 없어 조회 대상이 아닙니다."))
            continue

        rdir = pipe.raw_dir(vehicle, source)
        latest = _latest_partition(rdir)
        if latest is None:
            checks.append(_check(
                f"{source} raw 파티션", FAIL,
                f"{pipe.display_path(rdir)} 에 parquet 이 없습니다 — 조회가 한 번도 성공하지 못했습니다.",
                fix="알람 탭 → 작업 큐/재시도 큐에서 실패 사유(권한·테이블명·컬럼)를 확인하세요."))
            continue

        df = _read_head(latest)
        if df is None:
            checks.append(_check(f"{source} raw 파티션", FAIL,
                                 f"{latest.name} 을 읽지 못했습니다 (파일이 손상됐을 수 있음).",
                                 view=_view(pipe, latest, label=f"{source} raw 열기")))
            continue

        part_date = latest.parent.name[5:]
        view = _view(pipe, latest, label=f"{source} raw 열기")
        rows = _row_count(latest)
        rows = df.height if rows is None else rows
        checks.append(_check(
            f"{source} raw 파티션", OK if rows else FAIL,
            f"date={part_date} · {rows:,} 행 · {df.width} 열"
            + ("" if rows else " — 행이 0건입니다 (쿼리 조건이 아무것도 못 잡았습니다)"),
            view=view,
            fix="" if rows else "제품 탭의 조회 파라미터(process_id/line_id·lot prefix)를 확인하세요."))

        # 조회 컬럼이 실제로 다 왔는가 — 사내 테이블에 없는 열을 넣으면 조용히 빠지거나
        # 조회 자체가 통째로 실패한다. 어느 쪽인지는 여기서만 보인다.
        want = [c for c in (sc.get("columns") or [])]
        missing = [c for c in want if c not in df.columns]
        checks.append(_check(
            f"{source} 조회 컬럼", WARN if missing else OK,
            f"요청 {len(want)} 열 중 누락 {len(missing)}: {', '.join(missing)}" if missing
            else f"{len(want)} 열 모두 수신",
            view=view,
            fix="알람 탭 ⚙ '조회 컬럼' 에서 사내 테이블에 없는 열을 빼세요." if missing else ""))

        # 파티션 기준 열 — 여기가 틀리면 하루치가 통째로 엉뚱한 날짜로 들어간다
        try:
            tcol = pipe._time_col(source)
        except ValueError as e:
            tcol = None
            checks.append(_check(f"{source} 파티션 기준 열", FAIL, str(e),
                                 fix="알람 탭 ⚙ '파티션 기준 열' 을 조회 컬럼 중 하나로 바꾸세요."))
        if tcol and tcol in df.columns:
            days = (df.select(pl.col(tcol).cast(pl.Utf8).str.slice(0, 10))
                      .to_series().drop_nulls().unique().to_list())
            off = sorted(d for d in days if d and d != part_date)
            checks.append(_check(
                f"{source} 파티션 기준 열", WARN if off else OK,
                f"{tcol} → date={part_date}"
                + (f" · 다른 날짜 {len(off)}종 섞임: {', '.join(off[:5])}" if off
                   else " (전 행이 파티션 날짜와 일치)"),
                view=_view(pipe, latest,
                           f"SELECT {tcol}, COUNT(*) AS n FROM t GROUP BY {tcol} ORDER BY n DESC",
                           f"{source} 기준 열 분포 보기")))
        elif tcol:
            checks.append(_check(f"{source} 파티션 기준 열", FAIL,
                                 f"{tcol} 이 raw 에 없습니다 — 저장 날짜를 신뢰할 수 없습니다.",
                                 view=view))

        # KEY 열이 비어 있으면 event/feature 에서 조용히 행이 사라진다
        for key in ("root_lot_id", "wafer_id"):
            if key not in df.columns:
                checks.append(_check(f"{source} {key}", FAIL, "KEY 열이 없습니다.", view=view))
                continue
            nulls = int(df.select(pl.col(key).is_null().sum()).item())
            if df.height:
                checks.append(_check(
                    f"{source} {key}", WARN if nulls else OK,
                    f"null {nulls:,}/{df.height:,} 행" if nulls else "null 없음",
                    view=_view(pipe, latest, f"SELECT * FROM t WHERE {key} IS NULL LIMIT 200",
                               f"{key} null 행 보기") if nulls else view))

    return _stage("raw", "1. raw 수집",
                  "사내 API 조회가 실제로 나가고, 받은 데이터가 맞는 날짜 파티션에 저장됐는지",
                  checks)


# ══════════════════════════════════════════════════════════
# 2) event — 매칭이 실제로 붙는가
# ══════════════════════════════════════════════════════════
def _diag_event(pipe, vehicle: str) -> dict:
    checks: list[dict] = []
    cfg = pipe.vehicle_cfg(vehicle)
    prefix = str(cfg.get("event_lot_startwith") or "")

    # 매칭 파일 — 이게 비면 event 는 0행이 되고 전 단계가 조용히 빈다
    mf = pipe.matching_file("FAB")
    try:
        smap = pipe.step_map(vehicle)
    except Exception as e:
        smap = None
        checks.append(_check("vehicle_matching", FAIL, str(e)[:200],
                             view=_config_view(pipe, mf, "매칭 파일 열기") if mf else None))
    if smap is not None:
        steps = set(smap["step_id"].cast(pl.Utf8).str.strip_chars().to_list())
        checks.append(_check(
            "vehicle_matching", FAIL if not steps else OK,
            f"{pipe.display_path(mf) if mf else '(경로 없음)'} · 이 제품 step {len(steps)}건"
            + ("" if steps else " — 이 vehicle 행이 하나도 없습니다"),
            view=_config_view(pipe, mf, "매칭 파일 열기") if mf else None,
            fix="" if steps else "flow 매칭알람에서 step 을 승인하거나 Vehicle_matching.csv 의 vehicle 값을 확인하세요."))
    else:
        steps = set()

    for source in pipe.sources_cfg():
        if not pipe.event_enabled(source):
            continue
        rdir, edir = pipe.raw_dir(vehicle, source), pipe.event_dir(vehicle, source)
        raw_latest = _latest_partition(rdir)
        ev_latest = _latest_partition(edir)

        if raw_latest is None:
            checks.append(_check(f"{source} event", SKIP, "raw 가 없어 event 를 판단할 수 없습니다."))
            continue
        if ev_latest is None:
            checks.append(_check(
                f"{source} event 파티션", FAIL,
                f"raw 는 있는데 {pipe.display_path(edir)} 가 비어 있습니다.",
                fix="아래 매칭률/prefix 검사를 보세요 — 필터가 전부 걸러냈을 가능성이 큽니다."))

        raw = _read_head(raw_latest)
        if raw is None:
            continue
        ev_view = _view(pipe, ev_latest, label=f"{source} event 열기") if ev_latest else None

        # root_lot prefix — 잘못 잡으면 event 가 통째로 0행이 된다
        if prefix and "root_lot_id" in raw.columns and raw.height:
            hit = int(raw.select(pl.col("root_lot_id").cast(pl.Utf8)
                                 .str.starts_with(prefix).sum()).item())
            pct = hit / raw.height * 100
            checks.append(_check(
                f"{source} root_lot prefix", OK if hit else FAIL,
                f"'{prefix}' 로 시작하는 행 {hit:,}/{raw.height:,} ({pct:.1f}%)",
                view=_view(pipe, raw_latest,
                           "SELECT root_lot_id, COUNT(*) AS n FROM t GROUP BY root_lot_id "
                           "ORDER BY n DESC LIMIT 200", f"{source} raw 의 root_lot 목록"),
                fix="" if hit else f"제품 탭의 event_lot_startwith('{prefix}') 가 실제 lot 체계와 맞는지 확인하세요."))

        # step 매칭률 — "이벤트가 정확한가" 의 핵심. 여기가 낮으면 feature 가 텅 빈다
        if source in ("FAB", "VM") and "step_id" in raw.columns and raw.height and steps:
            uniq = [s for s in raw.select(pl.col("step_id").cast(pl.Utf8).str.strip_chars())
                    .to_series().drop_nulls().unique().to_list() if s]
            matched = [s for s in uniq if s in steps]
            miss = sorted(set(uniq) - steps)
            pct = (len(matched) / len(uniq) * 100) if uniq else 0.0
            status = OK if pct >= 80 else (WARN if pct > 0 else FAIL)
            checks.append(_check(
                f"{source} step 매칭률", status,
                f"raw step {len(uniq)}종 중 매칭 {len(matched)}종 ({pct:.0f}%)"
                + (f" · 미매칭 예: {', '.join(miss[:8])}" if miss else ""),
                view=_view(pipe, raw_latest,
                           "SELECT step_id, step_desc, COUNT(*) AS n FROM t "
                           "GROUP BY step_id, step_desc ORDER BY n DESC LIMIT 300",
                           f"{source} raw 의 step 목록"),
                fix="flow 매칭알람에서 미매칭 step 을 승인하면 다음 실행에 반영됩니다." if miss else ""))

        if ev_latest is None:
            continue
        ev = _read_head(ev_latest)
        if ev is None:
            checks.append(_check(f"{source} event 파티션", FAIL,
                                 f"{ev_latest.name} 을 읽지 못했습니다.", view=ev_view))
            continue

        ev_rows = _row_count(ev_latest)
        ev_rows = ev.height if ev_rows is None else ev_rows
        checks.append(_check(
            f"{source} event 행", OK if ev_rows else FAIL,
            f"date={ev_latest.parent.name[5:]} · {ev_rows:,} 행 · {ev.width} 열"
            + ("" if ev_rows else " — 필터를 통과한 행이 없습니다"),
            view=ev_view))

        # join 결과가 실제로 채워졌는가 — vehicle_matching 은 허용 목록이 아니라
        # step_id → function step 계약이다. 비어 있으면 feature 룰이 안 붙는다.
        if "step_desc" in ev.columns and ev.height:
            blank = int(ev.select(
                (pl.col("step_desc").is_null()
                 | (pl.col("step_desc").cast(pl.Utf8).str.strip_chars() == "")).sum()).item())
            checks.append(_check(
                f"{source} step_desc 채움", FAIL if blank == ev.height else
                (WARN if blank else OK),
                f"빈 값 {blank:,}/{ev.height:,} 행",
                view=_view(pipe, ev_latest,
                           "SELECT step_id, step_desc, COUNT(*) AS n FROM t "
                           "GROUP BY step_id, step_desc ORDER BY n DESC LIMIT 200",
                           f"{source} event 의 function step 분포")))

        # 매칭 파일이 바뀐 뒤 재생성이 안 됐으면 event 는 옛 버전 그대로다
        meta = {}
        mpath = edir / "_meta.json"
        if mpath.exists():
            try:
                meta = json.loads(mpath.read_text(encoding="utf-8"))
            except Exception:
                meta = {}
        cur_ver = pipe.event_version(vehicle, source)
        if meta.get("ver") and meta["ver"] != cur_ver:
            checks.append(_check(
                f"{source} 매칭 버전", WARN,
                "매칭 파일이 바뀐 뒤 event 가 재생성되지 않았습니다 "
                f"(적용본 {str(meta.get('sha'))[:8]} · {_fmt_ts(meta.get('ts'))}).",
                fix="알람 탭에서 ▶ 실행하면 해당 소스가 전체 재생성됩니다."))

    return _stage("event", "2. event 정확도",
                  "raw 가 매칭 테이블(step_id → function step)과 실제로 이어지는지",
                  checks)


# ══════════════════════════════════════════════════════════
# 3) feature — 매칭 테이블이 S3 에서 내려오고, 룰대로 산출되는가
# ══════════════════════════════════════════════════════════
_RULE_LABEL = {"fab": "fab.csv", "knob": "ppid_knob.csv", "mask": "mask.csv",
               "inline": "inline.csv", "vm": "vm.csv"}


def _fmt_ts(ts) -> str:
    try:
        return datetime.fromtimestamp(float(ts)).strftime("%m-%d %H:%M")
    except (TypeError, ValueError, OSError):
        return "-"


def _diag_feature(pipe, vehicle: str, s3_jobs=None, csv_sync=None) -> dict:
    checks: list[dict] = []

    # 3-a. 매칭/룰 테이블이 S3 에서 내려오고 있는가
    checks.extend(_matching_download_checks(pipe, s3_jobs, csv_sync))

    # 3-b. 룰 파일 자체
    gcfg = pipe.global_cfg()
    rule_paths = {"step_matching": pipe.matching_file("FAB")}
    for cat, rel in (gcfg.get("feature_rules") or {}).items():
        rule_paths[cat] = pipe.root / str(rel)
    rule_counts: dict[str, int] = {}
    for cat, path in rule_paths.items():
        if path is None or not Path(path).exists():
            checks.append(_check(f"룰 파일 {_RULE_LABEL.get(cat, cat)}", FAIL,
                                 f"{path} 이(가) 없습니다.",
                                 fix="탐색기 ⚙ S3 항목에서 이 파일의 다운로드 항목을 확인하세요."))
            continue
        try:
            n = pl.read_csv(path, infer_schema_length=0).height
        except Exception as e:
            checks.append(_check(f"룰 파일 {_RULE_LABEL.get(cat, cat)}", FAIL, str(e)[:160],
                                 view=_config_view(pipe, Path(path), "파일 열기")))
            continue
        rule_counts[cat] = n
        checks.append(_check(
            f"룰 파일 {_RULE_LABEL.get(cat, cat)}", FAIL if n == 0 else OK,
            f"{n:,} 행 · 수정 {_fmt_ts(Path(path).stat().st_mtime)}"
            + ("" if n else " — 비어 있습니다"),
            view=_config_view(pipe, Path(path), "파일 열기")))

    # 3-c. 산출된 feature
    fdir = pipe.feature_dir(vehicle)
    files = sorted(fdir.glob("*.parquet")) if fdir.is_dir() else []
    if not files:
        checks.append(_check("feature 산출물", FAIL,
                             f"{pipe.display_path(fdir)} 에 parquet 이 없습니다.",
                             fix="위의 event 단계 검사를 먼저 통과시켜야 합니다."))
    else:
        by_cat: dict[str, int] = {}
        for f in files:
            by_cat[f.name.split("_", 1)[0].lower()] = by_cat.get(
                f.name.split("_", 1)[0].lower(), 0) + 1
        checks.append(_check(
            "feature 산출물", OK,
            " · ".join(f"{k} {v}" for k, v in sorted(by_cat.items())) + f" (총 {len(files)} 파일)",
            view=_view(pipe, files[0], label="feature parquet 열기")))

        # 룰 수 대비 산출 수 — 룰은 있는데 값이 안 나온 feature 가 조용히 빠진다
        for cat, prefix in (("fab", "fab"), ("knob", "knob"), ("mask", "mask"),
                            ("inline", "inline"), ("vm", "vm")):
            want = rule_counts.get(cat)
            got = by_cat.get(prefix, 0)
            if not want:
                continue
            if got == 0:
                checks.append(_check(
                    f"{_RULE_LABEL.get(cat, cat)} 산출", FAIL,
                    f"룰 {want} 행이 있는데 {prefix.upper()}_ feature 가 하나도 안 나왔습니다.",
                    fix="event 의 function step(step_desc) 이 룰의 function_step 과 같은 값인지 확인하세요."))

        meta = {}
        mp = fdir / "_meta.json"
        if mp.exists():
            try:
                meta = json.loads(mp.read_text(encoding="utf-8"))
            except Exception:
                meta = {}
        cov = meta.get("sources") or {}
        if cov:
            span = " · ".join(f"{k} {v.get('days')}일 ({v.get('start')}~{v.get('end')})"
                              for k, v in sorted(cov.items()))
            checks.append(_check("feature 커버 구간", OK,
                                 f"{span} · 산출 {_fmt_ts(meta.get('ts'))}"))
        else:
            checks.append(_check("feature 커버 구간", WARN, "커버 구간 기록이 없습니다."))

    # 3-d. flow 로 나가는 ML_TABLE
    wide = pipe.wide_dir() / f"ML_TABLE_{vehicle}.parquet"
    if not wide.exists():
        checks.append(_check("ML_TABLE (내부)", FAIL,
                             f"{pipe.display_path(wide)} 이 없습니다.",
                             fix="feature 가 하나 이상 나와야 wide 단계가 돕니다."))
    else:
        df = _read_head(wide, 1)
        rows = _row_count(wide) or 0
        checks.append(_check(
            "ML_TABLE (내부)", OK if rows else FAIL,
            f"{wide.name} · {rows:,} 행 · {df.width if df is not None else 0} 열 · "
            f"{_fmt_ts(wide.stat().st_mtime)}"
            + ("" if rows else " — 행이 0건입니다"),
            view=_view(pipe, wide, label="ML_TABLE 열기")))

    product = str(pipe.vehicle_cfg(vehicle).get("product") or vehicle)
    flow_table = pipe.db_root() / f"ML_TABLE_{safe_filename(product)}.parquet"
    if not flow_table.exists():
        checks.append(_check(
            "ML_TABLE (flow 발행본)", FAIL,
            f"{flow_table.name} 이 db 루트에 없습니다 — flow 는 이 파일만 제품으로 인식합니다.",
            fix="알람 탭에서 ▶ 실행하면 wide 단계가 끝난 뒤 자동 발행됩니다 "
                "(POST /api/pipeline/wide/{vehicle} 로도 재발행)."))
    else:
        f_rows = _row_count(flow_table) or 0
        stale = wide.exists() and flow_table.stat().st_mtime < wide.stat().st_mtime
        checks.append(_check(
            "ML_TABLE (flow 발행본)", WARN if stale else (OK if f_rows else FAIL),
            f"{flow_table.name} · {f_rows:,} 행 · {_fmt_ts(flow_table.stat().st_mtime)}"
            + (" — 내부 ML_TABLE 보다 오래됐습니다 (발행이 밀림)" if stale else ""),
            view=_view(pipe, flow_table, label="flow 발행본 열기")))

    return _stage("feature", "3. feature · 매칭 테이블",
                  "매칭 테이블이 S3 에서 내려오고, 룰대로 feature/ML_TABLE 이 나오는지",
                  checks)


# ══════════════════════════════════════════════════════════
# 4) SEND_FORM — wide 를 prefix 그룹으로 쪼갠 최종 산출물
# ══════════════════════════════════════════════════════════
def _diag_send(pipe) -> dict:
    """전 제품 wide 를 합쳐 prefix 그룹(0.KNOB / 1.FAB(+MASK) / 2.VM / 3.INLINE)으로
    분리한 결과. **제품 하나가 아니라 전 제품 합산**이라 vehicle 인자가 없다.

    그룹이 비는 건 정상일 수도 있다(그 prefix 의 feature 가 아예 없는 경우) — 그래서
    'wide 에 해당 prefix 컬럼이 있는데 산출물이 없다' 일 때만 실패로 본다."""
    checks: list[dict] = []
    wide_files = sorted(pipe.wide_dir().glob("ML_TABLE_*.parquet"))
    if not wide_files:
        return _stage("send", "4. SEND_FORM · prefix 분리",
                      "wide 를 KNOB / FAB(+MASK) / VM / INLINE 로 쪼갠 최종 산출물",
                      [_check("wide form", FAIL,
                              "4.WIDE_FORM 에 ML_TABLE 이 없어 prefix 분리를 할 수 없습니다.",
                              fix="위 3단계를 먼저 통과시키세요.")])

    # wide 에 실제로 있는 컬럼 — 그룹이 비어야 정상인지 판단하는 기준
    cols: set[str] = set()
    newest_wide = 0.0
    for f in wide_files:
        try:
            cols |= set(pl.scan_parquet(f).collect_schema().names())
            newest_wide = max(newest_wide, f.stat().st_mtime)
        except Exception:
            continue
    checks.append(_check(
        "입력 wide form", OK,
        f"{len(wide_files)} 제품 · {', '.join(f.name for f in wide_files[:4])}"
        + (" …" if len(wide_files) > 4 else "") + f" · 최신 {_fmt_ts(newest_wide)}"))

    for group, prefixes in SEND_GROUPS.items():
        gname = group.split(".", 1)[-1]
        want = sorted(c for c in cols if any(c.startswith(p) for p in prefixes))
        out = pipe.send_dir() / group / f"{gname}_ML_TABLE.parquet"
        csv = pipe.send_dir() / group / f"{gname}_ML_TABLE.csv"
        label = f"{group} ({'+'.join(p.rstrip('_') for p in prefixes)})"

        if not out.exists():
            checks.append(_check(
                label, SKIP if not want else FAIL,
                "이 prefix 의 feature 가 없어 분리 대상이 아닙니다." if not want
                else f"wide 에 {len(want)} 개 컬럼이 있는데 {out.name} 이 없습니다.",
                fix="" if not want else "알람 탭 ▶ 전체 실행 또는 POST /api/pipeline/send-form"))
            continue

        df = _read_head(out, 1)
        got = [c for c in (df.columns if df is not None else []) if c not in WIDE_KEY]
        rows = _row_count(out) or 0
        missing = [c for c in want if c not in got]
        stale = out.stat().st_mtime < newest_wide
        status = FAIL if (not rows or missing) else (WARN if stale else OK)
        detail = (f"{rows:,} 행 · {len(got)} 컬럼 · {_fmt_ts(out.stat().st_mtime)}"
                  + (f" · csv {'있음' if csv.exists() else '없음'}"))
        if not rows:
            detail += " — 행이 0건입니다"
        if missing:
            detail += f" · wide 에 있는데 빠진 컬럼 {len(missing)}: {', '.join(missing[:6])}"
        elif stale:
            detail += " — wide 보다 오래됐습니다 (분리가 밀림)"
        checks.append(_check(
            label, status, detail,
            view=_view(pipe, out, label=f"{gname} 열기"),
            fix="알람 탭 ▶ 전체 실행 또는 POST /api/pipeline/send-form"
                if status == FAIL else ""))

    return _stage("send", "4. SEND_FORM · prefix 분리",
                  "wide 를 KNOB / FAB(+MASK) / VM / INLINE 로 쪼갠 최종 산출물 (전 제품 합산)",
                  checks)


# ══════════════════════════════════════════════════════════
# 5) S3 전송 — 만들어 놓고 안 보내면 flow 는 옛 데이터를 본다
# ══════════════════════════════════════════════════════════
_S3_OK = ("ok", "done", "success", "unchanged", "skipped")


def _diag_s3_upload(pipe, s3_jobs) -> dict:
    """Valve → S3 업로드 항목이 실제로 주기적으로 나가고 있는가.

    앞 네 단계가 전부 초록이어도 여기가 멈추면 flow 는 옛 데이터를 계속 본다 —
    산출과 전송은 다른 고장이다. 수동 전용 항목(interval 0)은 감시 대상이 아니다."""
    if s3_jobs is None:
        return _stage("s3", "5. S3 전송 (Valve → flow)", "",
                      [_check("전송 엔진", SKIP, "S3 전송 엔진이 연결되지 않았습니다.")])
    try:
        info = s3_jobs.list_with_status()
    except Exception as e:
        return _stage("s3", "5. S3 전송 (Valve → flow)", "",
                      [_check("전송 항목 조회", WARN, str(e)[:200])])

    checks: list[dict] = []
    uploads = [it for it in info.get("items") or [] if it.get("direction") == "upload"]
    if not info.get("auto_upload_enabled") and uploads:
        checks.append(_check("자동 업로드", FAIL, "S3 자동 업로드가 꺼져 있습니다.",
                             fix="탐색기 ⚙ → 자동 업로드를 켜세요."))
    if not uploads:
        checks.append(_check("업로드 항목", FAIL,
                             "Valve → S3 업로드 항목이 하나도 없습니다.",
                             fix="탐색기 ⚙ 에서 ML_TABLE·매칭알람 outbox 업로드를 등록하세요."))

    for it in sorted(uploads, key=lambda x: str(x.get("id"))):
        st = it.get("status") or {}
        last = str(st.get("last_status") or "")
        when = _fmt_ts(st.get("last_end"))
        try:
            interval = float(it.get("interval_min") or 0)
        except (TypeError, ValueError):
            interval = 0
        where = f"{it.get('root')}/{it.get('target')}".rstrip("/") + f" → {it.get('key')}"
        name = f"업로드 [{it.get('id')}]"

        if not it.get("enabled") or interval <= 0:
            checks.append(_check(name, SKIP, f"수동 전용 (주기 없음) · {where}"))
            continue
        if not it.get("s3_configured"):
            checks.append(_check(name, FAIL, f"S3 연결이 설정되지 않았습니다 · {where}",
                                 fix="설정 탭 → S3 (bucket·자격증명) 을 채우세요."))
            continue
        if not last:
            checks.append(_check(name, WARN, f"아직 한 번도 실행되지 않았습니다 · {where}",
                                 fix="탐색기 ⚙ 에서 ▶ 지금 실행으로 확인하세요."))
            continue
        if last not in _S3_OK:
            checks.append(_check(name, FAIL,
                                 f"{last} · {when} · {st.get('error') or where}",
                                 fix="탐색기 ⚙ 에서 ▶ 지금 실행으로 원인을 확인하세요."))
            continue
        # 성공했더라도 "보낼 로컬 파일이 마지막 성공보다 새로우면" 밀린 것이다
        stale = ""
        try:
            src = s3_jobs._resolve(it["root"], it["target"])
            local = _newest_local(src)
            if local and st.get("last_end") and local > st["last_end"]:
                stale = f" — 로컬이 더 새롭습니다({_fmt_ts(local)}) · 다음 주기({interval:g}분) 대기 중"
        except Exception:
            pass
        checks.append(_check(name, WARN if stale else OK,
                             f"{last} · {when} · {interval:g}분 주기 · {where}{stale}"))

    return _stage("s3", "5. S3 전송 (Valve → flow)",
                  "만든 산출물이 실제로 S3 로 계속 나가고 있는지 (전송은 산출과 다른 고장이다)",
                  checks)


def _newest_local(path: Path) -> float | None:
    if path.is_file():
        try:
            return path.stat().st_mtime
        except OSError:
            return None
    if not path.is_dir():
        return None
    newest = None
    for f in path.rglob("*"):
        if not f.is_file() or f.name.endswith(".tmp"):
            continue
        try:
            m = f.stat().st_mtime
        except OSError:
            continue
        if newest is None or m > newest:
            newest = m
    return newest


def _matching_download_checks(pipe, s3_jobs, csv_sync) -> list[dict]:
    """매칭 csv 다운로드 경로 점검 — s3_jobs(현행) 우선, 없으면 csv_sync(구 경로)."""
    out: list[dict] = []
    seen = False
    if s3_jobs is not None:
        try:
            info = s3_jobs.list_with_status()
        except Exception as e:
            return [_check("매칭 테이블 S3 다운로드", WARN, f"항목 조회 실패: {str(e)[:160]}")]
        downloads = [it for it in info.get("items") or []
                     if it.get("direction") == "download"]
        if downloads:
            seen = True
        if not info.get("auto_download_enabled") and downloads:
            out.append(_check("자동 다운로드", WARN, "S3 자동 다운로드가 꺼져 있습니다.",
                              fix="탐색기 ⚙ → 자동 다운로드를 켜세요."))
        for it in downloads:
            st = it.get("status") or {}
            last = st.get("last_status")
            when = _fmt_ts(st.get("last_end"))
            if not it.get("s3_configured"):
                status, detail = FAIL, "S3 연결이 설정되지 않았습니다 (bucket/자격증명)."
            elif last in (None, ""):
                status, detail = WARN, "아직 한 번도 실행되지 않았습니다."
            elif last in ("ok", "done", "success", "unchanged"):
                status, detail = OK, f"{last} · {when} · {it.get('key')}"
            else:
                status, detail = FAIL, f"{last} · {when} · {st.get('error') or it.get('key')}"
            out.append(_check(f"S3 다운로드 [{it.get('id')}]", status, detail,
                              fix="탐색기 ⚙ 에서 ▶ 지금 실행으로 원인을 확인하세요."
                                  if status == FAIL else ""))
    if not seen and csv_sync is not None:
        try:
            cfg, status = csv_sync.load_config(), csv_sync.load_status()
        except Exception as e:
            return [_check("매칭 테이블 S3 다운로드", WARN, f"조회 실패: {str(e)[:160]}")]
        if not cfg.get("enabled"):
            out.append(_check("매칭 테이블 S3 다운로드", WARN,
                              "csv_sync 가 꺼져 있고 S3 다운로드 항목도 없습니다.",
                              fix="탐색기 ⚙ 에서 매칭 csv 다운로드 항목을 등록하세요."))
        for f in cfg.get("files") or []:
            st = status.get(f.get("key")) or {}
            s = st.get("status")
            out.append(_check(
                f"csv_sync [{f.get('key')}]",
                OK if s in ("updated", "unchanged") else (WARN if s is None else FAIL),
                f"{s or '미실행'} · {_fmt_ts(st.get('ts'))} · {st.get('error') or f.get('dest')}"))
        seen = bool(cfg.get("files"))
    if not out and not seen:
        out.append(_check("매칭 테이블 S3 다운로드", FAIL,
                          "매칭 csv 를 내려받는 S3 항목이 하나도 없습니다 — "
                          "룰북이 flow 판정을 반영하지 못합니다.",
                          fix="탐색기 ⚙ S3 항목에서 flow/artifacts/matching 다운로드를 등록하세요."))
    return out


# ══════════════════════════════════════════════════════════
def diagnose(pipe, vehicle: str, s3_jobs=None, csv_sync=None) -> dict:
    """제품 하나의 단계별 진단. 한 단계가 실패해도 뒤 단계까지 전부 돈다 —
    "어디까지 되는지" 를 한 번에 봐야 하기 때문."""
    stages = []
    for fn in (lambda: _diag_raw(pipe, vehicle),
               lambda: _diag_event(pipe, vehicle),
               lambda: _diag_feature(pipe, vehicle, s3_jobs, csv_sync),
               lambda: _diag_send(pipe),
               lambda: _diag_s3_upload(pipe, s3_jobs)):
        try:
            stages.append(fn())
        except Exception as e:      # 진단이 터져도 나머지 단계는 계속
            stages.append(_stage("error", "진단 실패", "",
                                 [_check("예외", FAIL, f"{type(e).__name__}: {str(e)[:300]}")]))
    return {
        "vehicle": vehicle,
        "product": str(pipe.vehicle_cfg(vehicle).get("product") or vehicle),
        "ts": time.time(),
        "db_root": str(pipe.db_root()),
        "stages": stages,
        "status": _worst(s["status"] for s in stages),
        # 첫 번째로 막힌 단계 — "여기부터 보세요"
        "blocked_at": next((s["key"] for s in stages if s["status"] == FAIL), None),
    }
