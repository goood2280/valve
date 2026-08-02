"""
Valve · stage_health
--------------------
제품(vehicle) × 소스 × 단계(raw → event → feature → wide)가 **언제까지의 데이터를
언제 만들었는지**를 한 곳에서 계산한다. 여기서 나온 값이 두 곳에 쓰인다:

  1. 매칭알람 payload — flow 가 "이 제품의 raw 가 며칠째 안 늘고 있다" 를 알 수 있게
     `stage_stall` 알람 행 + `health` 블록으로 실려 나간다 (alert_store).
  2. 진단 탭 — 단계별 신호등과 정체 사유 표시 (routers/pipeline.py).

**정체(stall) 판정 기준**

단계마다 "최신"의 의미가 다르다 — raw/event 는 데이터의 날짜(date= 파티션),
feature/wide 는 산출물이 만들어진 시각이다. 둘을 섞으면 "매 시간 도는데 데이터는
3일째 안 들어오는" 상태를 놓친다. 그래서 단계별로 다음 두 축을 따로 본다:

  · lag_days      : 오늘 − 최신 데이터 날짜. 오늘 파티션은 하루가 지나야 차므로
                    기본 임계 1일은 "어제까지는 정상, 그저께가 최신이면 정체".
  · behind_days   : 앞 단계보다 며칠 뒤처졌는가. raw 는 매일 들어오는데 event 만
                    멈춘 경우(매칭 실패·재생성 중단)를 여기서 잡는다.
  · age_hours     : 마지막 산출 시각으로부터 경과 시간. feature/wide 는 날짜 축이
                    없어(파일에 날짜 컬럼이 없다) 이 값이 유일한 진행 신호다.

한 단계라도 위 셋 중 하나가 임계를 넘으면 stalled=True 로 올린다. 사유는 사람이
읽을 문장으로 `reason` 에 담는다 — flow 화면과 알람 본문이 그대로 쓴다.

설정은 `pipeline.yaml stall_alert` (알람 탭 ⚙). config/ 는 seed-only 라 기존
설치에는 키가 없다 — 기본값을 코드에 둬야 업그레이드만으로 동작한다.
"""
from __future__ import annotations

import json
import time
from datetime import date, datetime
from pathlib import Path

from backend.core.feature_pipeline import SEND_GROUPS, safe_filename

ALL_STAGES = ("raw", "event", "feature", "wide", "flow", "send", "s3")

STALL_DEFAULT = {
    "enabled": True,
    "threshold_days": 1,     # 이 일수를 **넘게** 안 늘면 정체 (1 = 그저께가 최신이면 알람)
    "stages": list(ALL_STAGES),
}

STAGE_LABEL = {
    "raw": "raw 수집",
    "event": "event DB화",
    "feature": "feature 산출",
    "wide": "ML_TABLE(내부)",
    "flow": "flow 발행본",
    "send": "SEND_FORM(prefix 분리)",
    "s3": "S3 전송",
}

# 단계 체인 — 뒤 단계가 오래된 게 "자기 문제" 인지 "앞 단계가 밀린 여파" 인지를
# 가르는 기준. 여기 없는 단계(raw)는 언제나 자기 문제다.
UPSTREAM = {
    "event": "raw",
    "feature": "event",
    "wide": "feature",
    "flow": "wide",
    "send": "wide",
}


def stall_cfg(gcfg: dict | None) -> dict:
    """pipeline.yaml stall_alert (미설정 시 코드 기본값)."""
    cfg = dict(STALL_DEFAULT)
    cfg["stages"] = list(STALL_DEFAULT["stages"])
    raw = (gcfg or {}).get("stall_alert")
    if isinstance(raw, dict):
        cfg["enabled"] = bool(raw.get("enabled", True))
        try:
            cfg["threshold_days"] = max(1, min(60, int(raw.get("threshold_days") or 1)))
        except (TypeError, ValueError):
            pass
        if isinstance(raw.get("stages"), list):
            picked = {str(s).strip().lower() for s in raw["stages"]}
            got = [s for s in ALL_STAGES if s in picked]
            cfg["stages"] = got or list(STALL_DEFAULT["stages"])
    return cfg


# ── 파일시스템 관찰 (읽기 전용 — 파이프라인이 도는 중에도 안전) ──
def _partition_dates(root: Path) -> list[str]:
    """실제 parquet 이 있는 date= 파티션의 날짜만 (빈 폴더는 진행으로 치지 않는다)."""
    if not root.is_dir():
        return []
    out = []
    for d in root.glob("date=*"):
        try:
            if any(d.glob("*.parquet")):
                out.append(d.name[5:])
        except OSError:
            continue
    return sorted(out)


def _newest_mtime(root: Path, pattern: str = "date=*/*.parquet") -> float | None:
    if not root.is_dir():
        return None
    newest = None
    for f in root.glob(pattern):
        try:
            m = f.stat().st_mtime
        except OSError:
            continue
        if newest is None or m > newest:
            newest = m
    return newest


def _parse_date(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return datetime.strptime(str(s)[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _days_between(newer: date, older: str | None) -> int | None:
    d = _parse_date(older)
    return None if d is None else (newer - d).days


def _hours_since(ts: float | None, now: float) -> float | None:
    return None if not ts else max(0.0, (now - ts) / 3600.0)


def _mtime(path: Path) -> float | None:
    try:
        return path.stat().st_mtime
    except OSError:
        return None


def _behind_ts(upstream_ts: float | None, own_ts: float | None) -> int | None:
    """앞 단계 산출물보다 며칠 뒤에 만들어졌나 (날짜 컬럼이 없는 단계용).
    앞 단계가 먼저면 0 — 뒤처지지 않았다는 뜻이다."""
    if not upstream_ts or not own_ts:
        return None
    return max(0, int((upstream_ts - own_ts) // 86400))


def _row(stage: str, source: str, **kw) -> dict:
    row = {
        "stage": stage,
        "source": source,
        "label": STAGE_LABEL.get(stage, stage),
        # global = 제품 하나가 아니라 전 제품 합산 산출물 (SEND_FORM). 알람 id 도
        # vehicle 을 쓰지 않으므로 소비 측에서 id 중복 제거가 필요하다.
        "scope": "vehicle",
        "latest_date": None,
        "lag_days": None,
        "behind_of": None,
        "behind_days": None,
        "last_write_ts": None,
        "age_hours": None,
        "partitions": 0,
        "stalled": False,
        "cascade": False,
        "reason": "",
    }
    row.update(kw)
    return row


def _mark(row: dict, threshold_days: int, *, use_age: bool) -> dict:
    """임계 초과 여부 판정 + 사람이 읽는 사유. 판정 규칙을 한 곳에 모아 둔다.

    `cascade` — 이 단계 자체는 앞 단계를 제때 따라가고 있는데, 앞 단계가 밀려서
    같이 오래된 경우. raw 가 3일 밀리면 event·feature 도 자동으로 3일 오래됐다고
    나오는데, 그 셋을 다 알람으로 보내면 원인 하나에 알람이 셋 뜬다. 현황(health)
    에는 그대로 두고 **알람 행은 원인 단계에서만** 만든다 (stall_alerts)."""
    reasons = []
    own = False       # 이 단계 자신의 문제인가 (앞 단계 밀림으로 설명되지 않는가)
    if row.get("missing"):
        reasons.append(row.get("missing_reason") or "산출물이 없습니다")
        own = True
    else:
        lag = row.get("lag_days")
        if lag is not None and lag > threshold_days:
            reasons.append(f"최신 데이터가 {lag}일 전({row.get('latest_date')})")
        behind = row.get("behind_days")
        if behind is not None and behind > threshold_days:
            reasons.append(f"{row.get('behind_of')} 보다 {behind}일 뒤처짐")
            own = True
        if use_age:
            age = row.get("age_hours")
            if age is not None and age > threshold_days * 24:
                reasons.append(f"마지막 산출이 {age / 24:.1f}일 전")
        # 오래됐다는 사실만으로는 원인이 아니다 — 앞 단계가 있는 단계는 "앞 단계보다
        # 뒤처졌는가"(behind)로만 원인을 가른다. 그렇게 하지 않으면 raw 하나가 밀렸을 때
        # event·feature·wide·flow·send 가 전부 자기 문제라며 알람을 낸다.
        if reasons and not own and row.get("behind_of") is None:
            own = True      # 앞 단계가 없는 단계(raw)는 언제나 자기 문제다
    row["stalled"] = bool(reasons)
    row["cascade"] = bool(reasons) and not own
    row["reason"] = " · ".join(reasons)
    if row["cascade"]:
        row["reason"] += f" (앞 단계 {row.get('behind_of')} 가 밀린 여파)"
    return row


_S3_OK = ("ok", "done", "success", "unchanged", "skipped")


def _s3_rows(pipe, s3_jobs, thr: int, now: float) -> list[dict]:
    """주기 전송 항목의 진행 상태. **자동 주기가 걸린 항목만** 본다 —
    interval 0(수동 전용)이나 꺼둔 항목을 감시하면 영원히 빨간불이다.

    두 가지를 본다:
      · 마지막 성공이 언제인가 (전송이 아예 안 도는가)
      · 보낼 로컬 파일이 마지막 성공보다 새로운가 (밀려 있는가)
    로컬이 안 변하면 전송은 '보낼 게 없어' 빨리 끝나지만 last_end 는 갱신되므로,
    여기가 오래됐다는 건 전송 자체가 멈췄다는 뜻이다."""
    try:
        info = s3_jobs.list_with_status()
    except Exception:
        return []
    rows = []
    for it in info.get("items") or []:
        auto = (info.get("auto_download_enabled") if it.get("direction") == "download"
                else info.get("auto_upload_enabled"))
        try:
            interval = float(it.get("interval_min") or 0)
        except (TypeError, ValueError):
            interval = 0
        if not (it.get("enabled") and interval > 0 and auto):
            continue          # 수동 전용/비활성 항목은 감시 대상이 아니다
        st = it.get("status") or {}
        ok_ts = st.get("last_end") if str(st.get("last_status") or "") in _S3_OK else None
        row = _row("s3", f"{it.get('direction')} {it.get('id')}", scope="global",
                   last_write_ts=ok_ts, partitions=1)
        row["age_hours"] = _hours_since(ok_ts, now)
        row["s3_key"] = it.get("key")
        row["s3_target"] = f"{it.get('root')}/{it.get('target')}".rstrip("/")
        if not it.get("s3_configured"):
            row["missing"] = True
            row["missing_reason"] = "S3 연결이 설정되지 않았습니다 (bucket/자격증명)"
        elif ok_ts is None:
            row["missing"] = True
            row["missing_reason"] = (
                f"마지막 결과가 {st.get('last_status') or '미실행'} — 성공한 전송이 없습니다")
        elif it.get("direction") == "upload":
            # 보낼 로컬 파일이 마지막 성공보다 새로우면 그만큼 밀려 있다
            try:
                src = s3_jobs._resolve(it["root"], it["target"])
            except Exception:
                src = None
            local = _newest_local(src) if src else None
            row["behind_of"] = "로컬 산출물"
            row["behind_days"] = _behind_ts(local, ok_ts)
        rows.append(_mark(row, thr, use_age=True))
    return rows


def _newest_local(path: Path) -> float | None:
    """파일이면 그 mtime, 폴더면 그 아래에서 가장 최근 mtime."""
    if path.is_file():
        return _mtime(path)
    if not path.is_dir():
        return None
    newest = None
    for f in path.rglob("*"):
        if not f.is_file() or f.name.endswith(".tmp"):
            continue
        m = _mtime(f)
        if m is not None and (newest is None or m > newest):
            newest = m
    return newest


def stage_health(pipe, vehicle: str, today: date | None = None,
                 now: float | None = None, s3_jobs=None) -> dict:
    """제품 하나의 단계별 진행 상태. 파일시스템만 본다 (parquet 을 열지 않는다).

    반환:
      {vehicle, product, ts, today, threshold_days, enabled,
       stages: [row…], stalled: [row…], stalled_count: int}
    """
    today = today or date.today()
    now = now if now is not None else time.time()
    cfg = stall_cfg(pipe.global_cfg())
    thr = cfg["threshold_days"]
    want = set(cfg["stages"])

    try:
        product = str(pipe.vehicle_cfg(vehicle).get("product") or vehicle)
    except Exception:
        product = vehicle

    rows: list[dict] = []
    raw_latest: dict[str, str | None] = {}
    event_latest: dict[str, str | None] = {}

    for source in pipe.sources_cfg():
        # reformatter 소스(ET)에서 이 vehicle 의 REAL 항목이 하나도 없으면 raw 를
        # 아예 만들지 않는 게 정상이다 — 없는 걸 정체로 알람하지 않는다.
        try:
            if pipe.reformatter_items(vehicle, source) == []:
                continue
        except Exception:
            pass

        # ── raw ──
        rdir = pipe.raw_dir(vehicle, source)
        dates = _partition_dates(rdir)
        raw_latest[source] = dates[-1] if dates else None
        if "raw" in want:
            row = _row("raw", source,
                       latest_date=raw_latest[source],
                       lag_days=_days_between(today, raw_latest[source]),
                       last_write_ts=_newest_mtime(rdir),
                       partitions=len(dates))
            row["age_hours"] = _hours_since(row["last_write_ts"], now)
            if not dates:
                row["missing"] = True
                row["missing_reason"] = "raw 파티션이 하나도 없습니다"
            rows.append(_mark(row, thr, use_age=False))

        # ── event (event 를 만드는 소스만) ──
        if not pipe.event_enabled(source):
            continue
        edir = pipe.event_dir(vehicle, source)
        edates = _partition_dates(edir)
        event_latest[source] = edates[-1] if edates else None
        if "event" in want:
            row = _row("event", source,
                       latest_date=event_latest[source],
                       lag_days=_days_between(today, event_latest[source]),
                       last_write_ts=_newest_mtime(edir),
                       partitions=len(edates))
            row["age_hours"] = _hours_since(row["last_write_ts"], now)
            # raw 는 들어오는데 event 만 멈춘 경우 — 매칭 실패/재생성 중단의 신호
            if raw_latest.get(source) and event_latest.get(source):
                row["behind_of"] = "raw"
                row["behind_days"] = max(0, (_parse_date(raw_latest[source])
                                             - _parse_date(event_latest[source])).days)
            if not edates:
                row["missing"] = True
                row["missing_reason"] = ("raw 는 있는데 event 파티션이 없습니다"
                                         if raw_latest.get(source)
                                         else "raw·event 모두 없습니다")
            rows.append(_mark(row, thr, use_age=False))

    # ── feature (제품 단위 산출. 소스별 커버 구간은 _meta.json 이 정답) ──
    # build_ts 는 뒤 단계(wide)의 "앞 단계 대비" 기준이라 stages 설정과 무관하게 읽는다.
    fdir = pipe.feature_dir(vehicle)
    meta = {}
    mp = fdir / "_meta.json"
    if mp.exists():
        try:
            meta = json.loads(mp.read_text(encoding="utf-8"))
        except Exception:
            meta = {}
    build_ts = meta.get("ts")
    cov = meta.get("sources") or {}
    n_files = len(list(fdir.glob("*.parquet"))) if fdir.is_dir() else 0

    if "feature" in want:
        if not n_files:
            rows.append(_mark(_row("feature", "", partitions=0, missing=True,
                                   missing_reason="feature parquet 이 없습니다"),
                              thr, use_age=False))
        else:
            for source, ev_latest in sorted(event_latest.items()):
                end = (cov.get(source) or {}).get("end")
                row = _row("feature", source,
                           latest_date=end,
                           lag_days=_days_between(today, end),
                           last_write_ts=build_ts,
                           partitions=n_files)
                row["age_hours"] = _hours_since(build_ts, now)
                if end and ev_latest:
                    row["behind_of"] = "event"
                    row["behind_days"] = max(
                        0, (_parse_date(ev_latest) - _parse_date(end)).days)
                if not end:
                    row["missing"] = True
                    row["missing_reason"] = f"{source} 를 담은 feature 커버 구간 기록이 없습니다"
                rows.append(_mark(row, thr, use_age=True))

    # ── 이후 단계는 날짜 축이 없다 (파일에 날짜 컬럼이 없다) — 산출 시각만 본다.
    #    feature → wide → {flow 발행본, SEND_FORM} 순서로 "앞 단계보다 뒤에 만들어졌나"
    #    를 이어 붙인다. 그래야 raw 하나가 밀렸을 때 뒤 단계 전부가 원인으로 잡히지 않는다.
    wide_ts = _mtime(pipe.wide_dir() / f"ML_TABLE_{vehicle}.parquet")
    if "wide" in want:
        row = _row("wide", "", last_write_ts=wide_ts, partitions=1 if wide_ts else 0,
                   behind_of="feature", behind_days=_behind_ts(build_ts, wide_ts))
        row["age_hours"] = _hours_since(wide_ts, now)
        if wide_ts is None:
            row["missing"] = True
            row["missing_reason"] = "ML_TABLE 이 아직 만들어지지 않았습니다"
        rows.append(_mark(row, thr, use_age=True))

    # ── flow 발행본 — flow 는 db_root 직하의 ML_TABLE_{product}.parquet 만 제품으로
    #    인식한다. 내부 wide 가 새로 만들어져도 이게 안 바뀌면 flow 는 옛 데이터를 본다.
    if "flow" in want:
        name = f"ML_TABLE_{safe_filename(product)}.parquet"
        ts = _mtime(pipe.db_root() / name)
        row = _row("flow", product, last_write_ts=ts, partitions=1 if ts else 0,
                   behind_of="wide", behind_days=_behind_ts(wide_ts, ts))
        row["age_hours"] = _hours_since(ts, now)
        if ts is None:
            row["missing"] = True
            row["missing_reason"] = f"{name} 이 db 루트에 없습니다 — flow 가 이 제품을 못 본다"
        rows.append(_mark(row, thr, use_age=True))

    # ── SEND_FORM — wide 를 prefix(KNOB/FAB+MASK/VM/INLINE) 그룹으로 쪼갠 산출물.
    #    전 제품을 합쳐 만들므로 제품 하나에 매이지 않는다 (scope=global) — 알람 id 에
    #    vehicle 을 넣지 않고, 소비 측이 id 로 중복을 제거한다.
    if "send" in want:
        newest_wide = _newest_mtime(pipe.wide_dir(), "ML_TABLE_*.parquet")
        for group in SEND_GROUPS:
            gname = group.split(".", 1)[-1]
            ts = _mtime(pipe.send_dir() / group / f"{gname}_ML_TABLE.parquet")
            row = _row("send", group, scope="global",
                       last_write_ts=ts, partitions=1 if ts else 0,
                       behind_of="wide", behind_days=_behind_ts(newest_wide, ts))
            row["age_hours"] = _hours_since(ts, now)
            if ts is None:
                row["missing"] = True
                row["missing_reason"] = (
                    f"{group}/{gname}_ML_TABLE.parquet 이 없습니다 — "
                    "prefix 분리가 아직 돌지 않았거나 해당 prefix 컬럼이 없습니다")
            rows.append(_mark(row, thr, use_age=True))

    # ── S3 전송 — 로컬이 전부 최신이어도 업로드가 멈추면 flow 는 옛 데이터를 본다.
    #    산출(위)과 전송(아래)은 서로 다른 고장이라 따로 본다.
    if "s3" in want and s3_jobs is not None:
        rows.extend(_s3_rows(pipe, s3_jobs, thr, now))

    stalled = [r for r in rows if r["stalled"]]
    roots = [r for r in stalled if not r["cascade"]]
    return {
        "vehicle": vehicle,
        "product": product,
        "ts": now,
        "today": today.isoformat(),
        "threshold_days": thr,
        "enabled": cfg["enabled"],
        "stages": rows,
        "stalled": stalled,
        "stalled_count": len(stalled),
        # 알람이 나가는 건 원인 단계뿐 — 앞 단계가 밀린 여파는 현황에만 남는다
        "root_causes": roots,
        "root_cause_count": len(roots),
    }


def stall_alerts(pipe, vehicle: str, today: date | None = None,
                 now: float | None = None, health: dict | None = None,
                 s3_jobs=None) -> list[dict]:
    """정체 단계를 매칭알람 행으로. 알람 id 는 (제품·단계·소스) 단위로 안정적이다 —
    같은 정체가 매 실행 새 알람으로 쌓이지 않고, 데이터가 다시 들어오면 사라진다.

    앞 단계가 밀린 여파(cascade)는 알람으로 만들지 않는다 — raw 가 3일 밀리면
    event·feature 도 같이 오래됐다고 나오는데, 원인 하나에 알람 셋이 뜨면
    무엇을 고쳐야 하는지가 오히려 안 보인다. 전 단계 현황은 payload.health 에 있다.

    `health` 를 넘기면 다시 계산하지 않는다 — 알람 목록/발행은 같은 현황을 두 번
    쓰므로(행 + payload.health), 파티션 디렉터리를 두 번 훑을 이유가 없다."""
    if health is None:
        health = stage_health(pipe, vehicle, today=today, now=now, s3_jobs=s3_jobs)
    if not health.get("enabled"):
        return []
    out = []
    for r in health.get("root_causes") or []:
        src = r["source"]
        # 전 제품 합산 산출물(SEND_FORM)은 vehicle 에 매이지 않는다 — 제품 수만큼
        # 같은 알람이 생기지 않도록 id 에서 vehicle 을 뺀다 (소비 측이 id 로 dedupe).
        key = "-" if r.get("scope") == "global" else health["vehicle"]
        out.append({
            "id": f"stall|{key}|{r['stage']}|{src}",
            "type": "stage_stall",
            "vehicle": health["vehicle"],
            "product": health["product"],
            "stage": r["stage"],
            "stage_label": r["label"],
            "source": src,
            "scope": r.get("scope") or "vehicle",
            "step_id": "", "step_desc": "", "ppid": "", "split": "",
            "eqp_id": "", "eqp_model": "",
            "rows": 0, "n_lots": 0,
            "latest_date": r["latest_date"],
            "lag_days": r["lag_days"],
            "behind_of": r["behind_of"],
            "behind_days": r["behind_days"],
            "age_hours": None if r["age_hours"] is None else round(r["age_hours"], 1),
            "last_write_ts": r["last_write_ts"],
            "threshold_days": health["threshold_days"],
            "cascade": r["cascade"],
            "reason": r["reason"],
        })
    return out
