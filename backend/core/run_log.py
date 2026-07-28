"""
Valve · run_log
---------------
파이프라인 실행 이력 — vehicle 1회 실행 = 1 레코드 (append-only JSONL).

두 가지 목적을 겸한다:
  1. 화면용 로그 — 제품별로 raw/event/feature/wide 각 단계가 몇 초 걸려
     무엇을 몇 건 만들었는지, 무엇이 실패했는지 꼼꼼히 남긴다.
  2. 스케줄 상태 — 제품별 마지막 실행 시각의 근거. 메모리에만 두면 재기동마다
     전 제품이 "실행할 때가 됐다"로 판정돼 매번 전량 재실행된다.

레코드 (한 줄):
  {ts, ended_ts, vehicle, mode, ok, elapsed_sec,
   stages: {raw:{sec, units, rows{src:n}, errors[]},
            event:{sec, sources{src:{raw_rows,event_rows,partitions,rebuilt}}},
            feature:{sec, counts{cat:n}, skipped[], agg_overrides[], event_dates},
            wide:{sec, rows, cols}},
   error?}
mode: schedule | loop | manual | rebuild

파일이 max_bytes 를 넘으면 뒤쪽(최신) 절반만 남기고 잘라낸다 — 실행 이력은
잃어도 되는 정보이고, 최신 것만 있으면 화면·스케줄 판정 모두 성립한다.
"""
from __future__ import annotations

import json
import threading
import time
from pathlib import Path

STAGES = ("raw", "event", "feature", "wide")


class RunLog:
    def __init__(self, path: Path, max_bytes: int = 4 * 1024 * 1024):
        self.path = Path(path)
        self.max_bytes = int(max_bytes)
        self._lock = threading.Lock()
        self.path.parent.mkdir(parents=True, exist_ok=True)

    # ── 쓰기 ──
    def append(self, rec: dict) -> dict:
        if not rec.get("severity"):
            rec["severity"] = ("critical" if rec.get("error") else
                               ("warning" if rec.get("ok") is False else "info"))
        line = json.dumps(rec, ensure_ascii=False, default=str)
        with self._lock:
            try:
                with open(self.path, "a", encoding="utf-8") as f:
                    f.write(line + "\n")
                self._trim_if_needed()
            except Exception:
                pass  # 로그 실패가 파이프라인을 멈추지는 않는다
        return rec

    def _trim_if_needed(self):
        try:
            if not self.path.exists() or self.path.stat().st_size <= self.max_bytes:
                return
            lines = self.path.read_text(encoding="utf-8").splitlines()
            keep = lines[len(lines) // 2:]
            tmp = self.path.with_suffix(self.path.suffix + ".tmp")
            tmp.write_text("\n".join(keep) + "\n", encoding="utf-8")
            tmp.replace(self.path)
        except Exception:
            pass

    # ── 읽기 ──
    def _all(self) -> list[dict]:
        if not self.path.exists():
            return []
        out = []
        try:
            for line in self.path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except Exception:
                    continue
        except Exception:
            return []
        return out

    def tail(self, limit: int = 100, vehicle: str | None = None,
             mode: str | None = None, failed_only: bool = False,
             severity: str | None = None) -> list[dict]:
        """최신순. vehicle/mode/실패 필터."""
        recs = self._all()
        if vehicle:
            recs = [r for r in recs if r.get("vehicle") == vehicle]
        if mode:
            recs = [r for r in recs if r.get("mode") == mode]
        if failed_only:
            recs = [r for r in recs if not r.get("ok")]
        if severity:
            recs = [r for r in recs if (r.get("severity") or
                    ("warning" if r.get("ok") is False else "info")) == severity]
        return recs[-max(1, int(limit)):][::-1]

    def last_started(self) -> dict[str, float]:
        """vehicle → 마지막 실행 시작 ts. 스케줄 due 판정의 기준."""
        out: dict[str, float] = {}
        for r in self._all():
            v, ts = r.get("vehicle"), r.get("ts")
            if v and ts:
                out[v] = max(float(ts), out.get(v, 0.0))
        return out

    def vehicle_summary(self, recent: int = 20) -> dict[str, dict]:
        """vehicle 별 최근 N회 요약 — 마지막 실행/성공률/평균 소요."""
        by: dict[str, list[dict]] = {}
        for r in self._all():
            if r.get("vehicle"):
                by.setdefault(r["vehicle"], []).append(r)
        out = {}
        for v, recs in by.items():
            recs = recs[-max(1, int(recent)):]
            ok = [r for r in recs if r.get("ok")]
            secs = [float(r.get("elapsed_sec") or 0) for r in recs]
            last = recs[-1]
            out[v] = {
                "runs": len(recs), "ok": len(ok), "failed": len(recs) - len(ok),
                "last_ts": last.get("ts"), "last_ok": bool(last.get("ok")),
                "last_mode": last.get("mode"), "last_error": last.get("error"),
                "avg_sec": round(sum(secs) / len(secs), 2) if secs else None,
            }
        return out
