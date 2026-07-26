"""
Valve · pipeline_runner
-----------------------
파이프라인 병렬 오케스트레이터 + 진행상황(stage) 추적 + 주기/루프 스케줄러.

기본 동작 (run_all):
  vehicle 을 순회하며 (vehicle_workers 만큼 동시에)
    1) raw     : (source × 1일) 유닛을 raw_workers 스레드풀로 병렬 쿼리
    2) event   : 5일치를 event DB 화 (소스별 매칭 필터)
    3) feature : event DB "전체" 를 대상으로 feature 산출 (특정 기간 아님)
    4) wide    : vehicle 의 feature 를 ML_TABLE 로 병합 (4.WIDE_FORM)
  전 vehicle 완료 후:
    5) send    : ML_TABLE 전체 병합 → KNOB/FAB(+MASK)/VM/INLINE 분리 (5.SEND_FORM)
  진행 중에는 self.progress 가 vehicle 별 현재 단계(raw/event/feature)를 담아
  /api/pipeline/progress 로 노출 → 백필이 지금 어느 DB 단계인지 실시간 확인.

스케줄 (제품별 독립):
  · vehicles.yaml 의 `runs_per_day` 로 제품마다 하루 실행 횟수를 따로 준다.
    (일 3회 = 8시간 간격 · 일 6회 = 4시간 간격 · 0 이면 자동 실행 안 함)
    값이 없는 제품은 전역 runtime.interval_hours 를 쓴다 (24/interval_hours 회).
  · 백그라운드 루프가 sched_tick_sec(기본 60초)마다 "지금 돌 때가 된 제품"만
    골라 run_all(vehicles=due) 한다. 마지막 실행 시각의 근거는 run_log —
    메모리에 두면 재기동마다 전 제품이 due 로 잡혀 매번 전량 재실행된다.
  · loop_enabled : 위 주기를 무시하고 전 제품을 쉬지 않고 반복 (개발/백필용)

실행 락 (_run_lock) — 산출물을 건드리는 **모든** 진입점이 공유한다:
  · 스케줄/루프/수동 전체 실행 (run_all)
  · 수동 단건 실행            (run_vehicle_once  ← POST /api/pipeline/run/{vehicle})
  · 매칭 csv 갱신 후 재생성   (rebuild_after_config_change ← csv_sync on_updated)
  · wide / send_form 단독 실행 (run_wide_once · run_send_form_once)
  락을 공유하지 않으면 두 스레드가 같은 2.EVENT_DB/{vehicle}/{source}/ 를 동시에
  rmtree + write 한다. 그때 생기는 손상 세 가지:
    (a) 한쪽이 rebuild 로 파티션을 지우는 사이 다른 쪽이 "이미 있음"으로 판정해
        건너뛴다 → event DB 에 날짜 구멍. 예외 없이 조용히 틀린 feature 가 나간다.
    (b) 같은 data.parquet 에 동시 write → 잘린 parquet → 다음 _load_event 가 실패.
    (c) _meta.json 을 나중에 쓴 쪽이 이긴다 → 옛 매칭 기준 event 에 새 ver 이
        찍히면 stale 로 안 잡혀 영영 재생성되지 않는다.
"""
from __future__ import annotations

import asyncio
import copy
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from backend.core.feature_pipeline import FeaturePipeline
from backend.core.run_log import RunLog
from backend.core.runtime_env import WorkerPlan, plan_workers

DAY_SEC = 86400.0
# 실행 금지 시간대 기본값 — pipeline.yaml 에 키가 없는 기존 설치에도 적용된다
QUIET_DEFAULT = ("00:00", "02:00")


class PipelineBusy(RuntimeError):
    """다른 실행(스케줄/수동/재생성)이 산출물을 쓰고 있어 지금은 진입 불가."""


class PipelineRunner:
    def __init__(self, pipe: FeaturePipeline, run_log: RunLog | None = None):
        self.pipe = pipe
        self.runs = run_log or RunLog(pipe.root / "logs" / "pipeline_runs.jsonl")
        self._bg_task: asyncio.Task | None = None
        self.last_run: dict | None = None
        self.last_rebuild: dict | None = None   # 매칭 갱신 재생성 결과 (조용한 실패 방지)
        self.on_vehicle_done = None  # Callable[[str, dict], None] — 알람 발행 훅
        self._lock = threading.Lock()       # progress 보호
        # 산출물(event/feature/wide/send)을 쓰는 모든 진입점의 공용 락 — 모듈 docstring 참조
        self._run_lock = threading.Lock()
        self._raw_sem = threading.BoundedSemaphore(3)  # 전역 raw 동시 상한 (run_all 시 재설정)
        self._loop_count = 0
        self.progress: dict = {"running": False, "mode": None, "loop": False,
                               "loop_iter": 0, "started": None, "ts": None, "vehicles": {}}

    # ── 설정/계획 ──
    def runtime_cfg(self) -> dict:
        return self.pipe.global_cfg().get("runtime") or {}

    def plan(self) -> WorkerPlan:
        return plan_workers(self.runtime_cfg())

    def schedule_enabled(self) -> bool:
        """자동 실행 마스터 스위치 — 켜져 있고 실제 주기를 가진 제품이 하나라도 있으면 True."""
        if not self.runtime_cfg().get("schedule_enabled"):
            return False
        return any(p["interval_sec"] > 0 for p in self.schedule_plan().values())

    def loop_enabled(self) -> bool:
        return bool(self.runtime_cfg().get("loop_enabled"))

    # ── 실행 금지 시간대 (quiet window) ──────────────────────
    # 야간 백업/사내 API 점검처럼 파이프라인이 돌면 안 되는 구간을 비워둔다.
    #   quiet_start/quiet_end : "HH:MM" (서버 로컬 시각) · start > end 면 자정을 넘는 구간
    #   대상은 자동 실행(스케줄·루프)뿐 — 사람이 직접 누른 수동 실행은 막지 않는다.
    #   이미 돌고 있는 실행은 중단하지 않는다 (중간에 끊으면 event 파티션이 반만 남는다).
    @staticmethod
    def parse_hhmm(text) -> int | None:
        """'HH:MM' → 자정 기준 분. 형식이 틀리면 None (24:00 = 1440 허용).

        yaml 을 손으로 고쳐 `quiet_start: 9:00` 처럼 따옴표 없이 적으면 YAML 1.1 의
        60진수 규칙에 걸려 정수 540(=9*60) 으로 읽힌다 — 그 값도 분으로 받아준다.
        (`09:00` 처럼 0 으로 시작하면 문자열로 남는다. UI 저장은 항상 문자열.)
        """
        if isinstance(text, int) and not isinstance(text, bool):
            return text if 0 <= text <= 1440 else None
        m = re.match(r"^\s*(\d{1,2}):(\d{2})\s*$", str(text or ""))
        if not m:
            return None
        h, mi = int(m.group(1)), int(m.group(2))
        if h > 24 or mi > 59 or (h == 24 and mi):
            return None
        return h * 60 + mi

    def quiet_cfg(self) -> tuple[bool, str, str]:
        """(사용여부, 시작, 종료). 키가 아예 없는 기존 설치도 기본값(00:00~02:00)을 받는다 —
        config/ 는 seed-only 라 업그레이드해도 pipeline.yaml 이 갱신되지 않기 때문.
        빈 문자열은 '지웠다' 는 뜻이므로 기본값으로 되살리지 않는다."""
        rt = self.runtime_cfg()
        return (bool(rt.get("quiet_enabled", True)),
                rt["quiet_start"] if "quiet_start" in rt else QUIET_DEFAULT[0],
                rt["quiet_end"] if "quiet_end" in rt else QUIET_DEFAULT[1])

    def quiet_window(self) -> tuple[int, int] | None:
        """(시작분, 종료분). 꺼져 있거나 값이 없거나 시작==종료(빈 구간)면 None."""
        enabled, start, end = self.quiet_cfg()
        if not enabled:
            return None
        s, e = self.parse_hhmm(start), self.parse_hhmm(end)
        if s is None or e is None or s == e:
            return None
        return s, e

    def quiet_now(self, now: float | None = None) -> bool:
        w = self.quiet_window()
        if not w:
            return False
        s, e = w
        lt = time.localtime(now if now is not None else time.time())
        cur = lt.tm_hour * 60 + lt.tm_min
        return (s <= cur < e) if s < e else (cur >= s or cur < e)

    def quiet_until(self, now: float | None = None) -> float | None:
        """지금이 금지 시간대면 해제 시각(epoch), 아니면 None."""
        w = self.quiet_window()
        if not w or not self.quiet_now(now):
            return None
        now = now if now is not None else time.time()
        lt = time.localtime(now)
        midnight = now - (lt.tm_hour * 3600 + lt.tm_min * 60 + lt.tm_sec)
        end = midnight + w[1] * 60
        return end if end > now else end + DAY_SEC

    def quiet_state(self, now: float | None = None) -> dict:
        """UI 표시용 — 설정값 + 지금 막고 있는지 + 언제 풀리는지."""
        enabled, start, end = self.quiet_cfg()
        w = self.quiet_window()
        return {
            "enabled": enabled,
            "start": str(start or ""),
            "end": str(end or ""),
            "valid": w is not None,
            "now": self.quiet_now(now),
            "until": self.quiet_until(now),
        }

    # ── 제품별 주기 ──────────────────────────────────────────
    def default_runs_per_day(self) -> float:
        """runs_per_day 가 없는 제품의 기본값 — 전역 interval_hours 에서 환산."""
        ih = float(self.runtime_cfg().get("interval_hours") or 0)
        return round(DAY_SEC / (ih * 3600), 4) if ih > 0 else 0.0

    def runs_per_day(self, vehicle_cfg: dict) -> tuple[float, str]:
        """(하루 실행 횟수, 출처). 0 이면 자동 실행 안 함."""
        rpd = vehicle_cfg.get("runs_per_day")
        if rpd is None or str(rpd).strip() == "":
            return self.default_runs_per_day(), "global"
        try:
            return max(0.0, float(rpd)), "vehicle"
        except (TypeError, ValueError):
            return self.default_runs_per_day(), "global"

    def schedule_plan(self, now: float | None = None) -> dict[str, dict]:
        """제품별 주기 현황 — 화면/스케줄러 공용.
        {vehicle: {runs_per_day, source, interval_sec, last_ts, next_ts, due, enabled}}"""
        now = now or time.time()
        last = self.runs.last_started()
        master = bool(self.runtime_cfg().get("schedule_enabled"))
        quiet_until = self.quiet_until(now)   # 금지 시간대면 해제 시각, 아니면 None
        out = {}
        for v, cfg in self.pipe.vehicles().items():
            rpd, src = self.runs_per_day(cfg or {})
            interval = DAY_SEC / rpd if rpd > 0 else 0.0
            t = last.get(v)
            next_ts = (t + interval) if (t and interval > 0) else (now if interval > 0 else None)
            # 금지 시간대에 걸린 예정은 해제 시각으로 밀린다 (건너뛰는 게 아니라 미뤄짐)
            if quiet_until and next_ts is not None and next_ts < quiet_until:
                next_ts = quiet_until
            out[v] = {
                "vehicle": v, "product": (cfg or {}).get("product"),
                "runs_per_day": rpd, "source": src,
                "interval_sec": interval,
                "interval_hours": round(interval / 3600, 2) if interval else 0,
                "last_ts": t, "next_ts": next_ts,
                "due": bool(interval > 0 and (t is None or now - t >= interval)
                            and not quiet_until),
                "quiet_blocked": bool(quiet_until and interval > 0),
                "enabled": bool(master and interval > 0),
            }
        return out

    def due_vehicles(self, now: float | None = None) -> list[str]:
        """지금 돌 때가 된 제품. 이력이 없으면(최초 기동) 즉시 due."""
        return [v for v, p in self.schedule_plan(now).items() if p["enabled"] and p["due"]]

    # ── progress ──
    def _prog(self, vehicle: str, **kw):
        with self._lock:
            self.progress.setdefault("vehicles", {}).setdefault(vehicle, {}).update(kw)
            self.progress["ts"] = time.time()

    def snapshot(self) -> dict:
        with self._lock:
            snap = copy.deepcopy(self.progress)
        if self.last_rebuild is not None:
            snap["last_rebuild"] = self.last_rebuild
        # running 만 보면 회차 사이(loop_gap_sec)·스케줄 tick 사이에 잠깐 False 가 되어
        # 화면이 "실행 중 ↔ 대기" 를 오간다. 루프/스케줄이 켜져 있다는 사실을 같이 실어
        # 프론트가 그 공백을 "실행 중(다음 회차 준비)" 으로 계속 표시하게 한다.
        try:
            rt = self.runtime_cfg()
        except Exception:
            rt = {}
        snap["loop_enabled"] = bool(rt.get("loop_enabled"))
        snap["schedule_enabled"] = bool(rt.get("schedule_enabled"))
        snap["busy"] = self._run_lock.locked()   # 재생성 등 락을 잡은 다른 작업 포함
        try:
            snap["quiet"] = self.quiet_state()
        except Exception:
            snap["quiet"] = {"enabled": False, "now": False}
        return snap

    def _raw_guarded(self, cfg, source, start, end, split) -> int:
        """전역 raw 세마포어를 잡고 raw 유닛 실행 — 전 vehicle 합쳐 동시 raw ≤ raw_api_max."""
        with self._raw_sem:
            return self.pipe._run_raw_unit(cfg, source, start, end, split)

    # ── 단일 vehicle: 병렬 raw → event → feature (단계별 progress + 실행 로그) ──
    def run_vehicle(self, vehicle: str, plan: WorkerPlan | None = None,
                    mode: str = "manual") -> dict:
        plan = plan or self.plan()
        t0 = time.time()
        # 단계별 소요/산출을 여기 모아 run_log 에 그대로 남긴다 (화면 로그의 원본)
        stages: dict[str, dict] = {}
        rec = {"ts": t0, "vehicle": vehicle, "mode": mode, "ok": False, "stages": stages}
        try:
            cfg = self.pipe.vehicle_cfg(vehicle)
            rec["product"] = cfg.get("product")
            units = self.pipe._raw_units(cfg)

            # 1) RAW — (source × 날짜) 병렬. 동시 실행은 전역 raw 세마포어가 제한(≤ raw_api_max).
            ts = time.time()
            self._prog(vehicle, stage="raw", raw_done=0, raw_total=len(units), source=None)
            rows: dict[str, int] = {}
            errors: list[dict] = []
            done = 0
            with ThreadPoolExecutor(max_workers=max(1, plan.raw_workers),
                                    thread_name_prefix=f"raw-{vehicle}") as ex:
                futs = {ex.submit(self._raw_guarded, cfg, *u): u for u in units}
                for f in as_completed(futs):
                    src, start, *_ = futs[f]
                    done += 1
                    try:
                        rows[src] = rows.get(src, 0) + f.result()
                    except Exception as e:
                        errors.append({"source": src, "date": str(start), "error": str(e)[:300]})
                    self._prog(vehicle, raw_done=done, source=src)
            stages["raw"] = {"sec": round(time.time() - ts, 2), "units": len(units),
                             "rows": rows, "errors": errors}

            # 2) EVENT
            ts = time.time()
            self._prog(vehicle, stage="event", source=None)
            event = self.pipe.run_event(vehicle)
            stages["event"] = {
                "sec": round(time.time() - ts, 2),
                "sources": {s: {k: e.get(k) for k in
                                ("raw_rows", "event_rows", "partitions", "rebuilt")}
                            for s, e in event.items()},
                "rebuilt": [s for s, e in event.items() if e.get("rebuilt")],
            }

            # 3) FEATURE — event DB 전체 대상
            ts = time.time()
            ev_dates = self.pipe.event_date_count(vehicle)
            self._prog(vehicle, stage="feature", event_dates=ev_dates)
            feature = self.pipe.run_feature(vehicle)
            stages["feature"] = {
                "sec": round(time.time() - ts, 2), "counts": feature["features"],
                "event_dates": ev_dates,
                "skipped": feature.get("skipped") or [],
                "agg_overrides": feature.get("agg_overrides") or [],
                "knob_miss": len(feature.get("knob_miss") or []),
                "knob_skip": len(feature.get("knob_skip") or []),
            }

            # 4) WIDE — feature 전부를 ML_TABLE 로 병합
            ts = time.time()
            self._prog(vehicle, stage="wide")
            wide = self.pipe.run_wide(vehicle)
            stages["wide"] = {"sec": round(time.time() - ts, 2),
                              **{k: wide.get(k) for k in ("rows", "features", "path")
                                 if k in wide}}

            self._prog(vehicle, stage="done", elapsed=round(time.time() - t0, 2))
            result = {
                "vehicle": vehicle, "product": cfg["product"],
                "raw_rows": rows, "raw_units": len(units), "errors": errors,
                "event": {s: e["event_rows"] for s, e in event.items()},
                "feature": feature["features"], "event_dates": ev_dates,
                "wide": wide,
                "elapsed_sec": round(time.time() - t0, 2),
            }
            rec["ok"] = not errors
            return result
        except Exception as e:
            rec["error"] = str(e)[:500]
            raise
        finally:
            rec["ended_ts"] = time.time()
            rec["elapsed_sec"] = round(rec["ended_ts"] - t0, 2)
            self.runs.append(rec)
            # 알람 발행 훅 — raw 유닛 일부 실패(ok=False)는 feature 까지 정상이라 발행하지만,
            # 중간에 터져 단계가 덜 돈 경우(error)는 옛 리포트를 발행하지 않는다.
            if self.on_vehicle_done and not rec.get("error"):
                try:
                    self.on_vehicle_done(vehicle, rec)
                except Exception:
                    pass
        return result

    # ── vehicle 순회 (중복 실행 방지). vehicles=None 이면 전 제품 ──
    def run_all(self, plan: WorkerPlan | None = None, mode: str = "manual",
                vehicles: list[str] | None = None) -> dict:
        if not self._run_lock.acquire(blocking=False):
            return {"ok": False, "skipped": "이미 실행 중", "progress": self.snapshot()}
        try:
            plan = plan or self.plan()
            t0 = time.time()
            known = list(self.pipe.vehicles().keys())
            vehicles = [v for v in (vehicles or known) if v in known]
            if not vehicles:
                return {"ok": True, "skipped": "대상 제품 없음", "mode": mode, "vehicles": {}}
            if mode == "loop":
                self._loop_count += 1
            with self._lock:
                self.progress = {"running": True, "mode": mode, "loop": self.loop_enabled(),
                                 "loop_iter": self._loop_count, "started": t0, "ts": t0,
                                 "vehicles": {v: {"stage": "queued"} for v in vehicles}}

            # 전역 raw 동시 상한 재설정 — 전 vehicle 합쳐 raw 쿼리 ≤ raw_workers(=raw_api_max).
            self._raw_sem = threading.BoundedSemaphore(max(1, plan.raw_workers))
            results: dict[str, dict] = {}
            with ThreadPoolExecutor(max_workers=plan.vehicle_workers,
                                    thread_name_prefix="vehicle") as ex:
                futs = {ex.submit(self.run_vehicle, v, plan, mode): v for v in vehicles}
                for f in as_completed(futs):
                    v = futs[f]
                    try:
                        results[v] = f.result()
                    except Exception as e:
                        results[v] = {"vehicle": v, "error": str(e)[:300]}
                        self._prog(v, stage="error", error=str(e)[:200])

            # 5) SEND FORM — 전 vehicle ML_TABLE 병합 → prefix 그룹 분리 (KNOB/FAB+MASK/VM/INLINE)
            with self._lock:
                self.progress["send_form"] = "running"
                self.progress["ts"] = time.time()
            try:
                send_form = self.pipe.run_send_form()
            except Exception as e:
                send_form = {"error": str(e)[:300]}
            with self._lock:
                self.progress["send_form"] = "error" if "error" in send_form else "done"

            summary = {
                "ok": all("error" not in r for r in results.values()),
                "mode": mode, "vehicles": results, "plan": plan.__dict__,
                "send_form": send_form,
                "elapsed_sec": round(time.time() - t0, 2), "ts": t0,
            }
            self.last_run = summary
            return summary
        finally:
            with self._lock:
                self.progress["running"] = False
                self.progress["ts"] = time.time()
            self._run_lock.release()

    # ── 락을 공유하는 단독 진입점들 ───────────────────────────────
    def _enter(self, what: str):
        """산출물 쓰기 구간 진입. 다른 실행 중이면 PipelineBusy."""
        if not self._run_lock.acquire(blocking=False):
            raise PipelineBusy(
                f"{what}: 다른 파이프라인 실행이 진행 중입니다 "
                f"(mode={self.snapshot().get('mode')}) — 끝난 뒤 다시 시도하세요")

    def run_vehicle_once(self, vehicle: str) -> dict:
        """수동 단건 실행 (raw→event→feature→wide→unmatched). 스케줄러와 락 공유.
        스케줄 실행과 같은 경로를 타므로 실행 로그·진행상황이 똑같이 남는다."""
        self.pipe.vehicle_cfg(vehicle)          # 없는 vehicle 이면 여기서 ValueError → 404
        self._enter(f"{vehicle} 실행")
        try:
            plan = self.plan()
            self._raw_sem = threading.BoundedSemaphore(max(1, plan.raw_workers))
            with self._lock:
                self.progress = {"running": True, "mode": "manual", "loop": False,
                                 "loop_iter": self._loop_count, "started": time.time(),
                                 "ts": time.time(), "vehicles": {vehicle: {"stage": "queued"}}}
            result = self.run_vehicle(vehicle, plan, mode="manual")
            try:
                result["unmatched"] = self.pipe.scan_unmatched(vehicle)
            except Exception as e:
                result["unmatched"] = {"error": str(e)[:300]}
            return result
        finally:
            with self._lock:
                self.progress["running"] = False
                self.progress["ts"] = time.time()
            self._run_lock.release()

    def run_wide_once(self, vehicle: str) -> dict:
        self._enter(f"{vehicle} wide")
        try:
            return self.pipe.run_wide(vehicle)
        finally:
            self._run_lock.release()

    def run_send_form_once(self) -> dict:
        self._enter("send form")
        try:
            return self.pipe.run_send_form()
        finally:
            self._run_lock.release()

    def rebuild_after_config_change(self, timeout: float = 1800) -> dict:
        """매칭 csv 갱신 후 전 vehicle event/feature/wide 재생성 + 알람 재발행.

        여기서는 skip 하지 않고 **기다린다** — 건너뛰면 옛 매칭 기준 산출물이
        그대로 남아 flow 로 나간다. 실행 중인 run_all 이 끝나면 이어서 돈다.
        결과는 last_rebuild 에 남긴다 (예전엔 예외를 조용히 삼켜 실패가 안 보였다).
        """
        t0 = time.time()
        if not self._run_lock.acquire(timeout=timeout):
            out = {"ok": False, "waited_sec": round(time.time() - t0, 1),
                   "error": f"실행 락 대기 시간 초과({timeout}s) — 재생성 건너뜀"}
            self.last_rebuild = out
            return out
        try:
            done, errors = [], {}
            for v in self.pipe.vehicles():
                try:
                    self._prog(v, stage="rebuild")
                    self.pipe.run_event(v)
                    self.pipe.run_feature(v)
                    wide = self.pipe.run_wide(v)
                    done.append(v)
                    self._prog(v, stage="done")
                    if self.on_vehicle_done:
                        self.on_vehicle_done(v, wide)
                except Exception as e:
                    errors[v] = str(e)[:300]
                    self._prog(v, stage="error", error=str(e)[:200])
            try:
                self.pipe.run_send_form()
            except Exception as e:
                errors["send_form"] = str(e)[:300]
            out = {"ok": not errors, "vehicles": done, "errors": errors,
                   "waited_sec": round(time.time() - t0, 1), "ts": t0}
            self.last_rebuild = out
            return out
        finally:
            self._run_lock.release()

    # ── 백그라운드 루프 (제품별 주기 / loop) ──
    async def _loop(self):
        """sched_tick_sec 마다 due 인 제품만 골라 돌린다.
        제품마다 runs_per_day 가 달라도 한 루프가 전부 커버한다."""
        while True:
            rt = self.runtime_cfg()
            # 실행 금지 시간대 — 자동/루프 모두 새 실행을 띄우지 않는다.
            # (이미 돌고 있던 실행은 그대로 끝까지 간다 — 중간에 끊으면 산출물이 반만 남는다)
            if self.quiet_now():
                await asyncio.sleep(30)
                continue
            if rt.get("loop_enabled"):
                try:
                    await asyncio.to_thread(self.run_all, None, "loop")
                except Exception:
                    pass
                await asyncio.sleep(max(2.0, float(rt.get("loop_gap_sec") or 3)))
                continue
            tick = max(10.0, float(rt.get("sched_tick_sec") or 60))
            if not rt.get("schedule_enabled"):
                await asyncio.sleep(5)    # 비활성 — 토글을 빠르게 감지
                continue
            try:
                due = self.due_vehicles()
                if due:
                    await asyncio.to_thread(self.run_all, None, "schedule", due)
            except Exception:
                pass
            await asyncio.sleep(tick)

    def start_background(self):
        if self._bg_task is None or self._bg_task.done():
            self._bg_task = asyncio.get_event_loop().create_task(self._loop())

    def stop_background(self):
        if self._bg_task and not self._bg_task.done():
            self._bg_task.cancel()
        self._bg_task = None
