"""
Valve · alert_store
-------------------
파이프라인 알람(미매칭 step · KNOB RO ppid)의 통합 리스트 + flow 와의 S3 순환.

순환 구조:
  1. Valve 파이프라인 실행 → 알람 생성(전체 스캔) → S3 `{alerts_prefix}/pipeline/{vehicle}.json` 발행
     + 발행 스냅샷을 로컬 메타(db/REPORTS/{vehicle}/alerts_published.json)로 저장
       (first_seen 계승 + delta new/resolved — event DB 갱신/재알람 판단 근거)
  2. flow 가 S3 에서 읽어 룰북/매칭테이블(버전관리)에 반영·조치:
     a) 매칭 csv 수정 → S3 업로드 → Valve csv_sync 가 내려받음 → 재실행 시 알람 자연 소멸
     b) 반영 불필요/보류 건 → `{alerts_prefix}/pipeline/ack.json` 에 상태 기록
  3. Valve 는 ack.json 을 읽어 해당 알람을 억제(suppressed) — 다시 알람하지 않음

알람 id (억제 단위 — split 이 바뀌어도 같은 건은 재알람 금지):
  미매칭 step : um|{vehicle}|{step_id}
  RO ppid     : ro|{vehicle}|{step_id}|{ppid}

ack.json: { "<id>": {"status": "미확인예정"|"반영불필요", "note": str, "by": str, "ts": float} }
status 를 지우면(또는 "active") 다시 활성. S3 미가용 시 로컬 캐시(logs/alerts_ack.json) 사용.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

SUPPRESS_STATUSES = ("미확인예정", "반영불필요")


class AlertStore:
    def __init__(self, pipe, s3_uploader, settings: dict, root: Path):
        self.pipe = pipe
        self.s3 = s3_uploader
        self.root = Path(root)
        self.prefix = ((settings.get("alerts") or {}).get("s3_prefix") or "valve-alerts").strip("/")

    def _ack_key(self) -> str:
        return f"{self.prefix}/pipeline/ack.json"

    def _ack_cache(self) -> Path:
        return self.root / "logs" / "alerts_ack.json"

    # ── 알람 생성 (vehicle 별 통합 행) ──
    def build(self, vehicle: str) -> list[dict]:
        rows: list[dict] = []
        try:
            unm = self.pipe.scan_unmatched(vehicle)
        except RuntimeError:
            unm = None
        if unm:
            by_step: dict[str, dict] = {}
            for x in unm["unmatched"]:
                g = by_step.setdefault(x["step_id"], {
                    "id": f"um|{vehicle}|{x['step_id']}",
                    "type": "unmatched_step",
                    "vehicle": vehicle, "product": unm["product"],
                    "step_id": x["step_id"], "step_desc": x.get("step_desc", ""),
                    "ppid": "", "split": "",
                    "eqp_id": set(), "eqp_model": set(),
                    "rows": 0, "n_lots": 0,
                })
                g["eqp_id"].add(x.get("eqp_id", ""))
                g["eqp_model"].add(x.get("eqp_model", ""))
                g["rows"] += x["rows"]
                g["n_lots"] = max(g["n_lots"], x.get("n_lots", 0))
            for g in by_step.values():
                g["eqp_id"] = ", ".join(sorted(filter(None, g["eqp_id"])))
                g["eqp_model"] = ", ".join(sorted(filter(None, g["eqp_model"])))
                rows.append(g)

        by_ppid: dict[str, dict] = {}
        for m in (self.pipe.load_report(vehicle, "knob_miss") or []):
            key = f"ro|{vehicle}|{m['step_id']}|{m['ppid']}"
            g = by_ppid.setdefault(key, {
                "id": key, "type": "ro_ppid",
                "vehicle": vehicle, "product": self.pipe.vehicle_cfg(vehicle)["product"],
                "step_id": m["step_id"], "step_desc": m.get("step_desc", ""),
                "ppid": m["ppid"], "split": [],
                "eqp_id": "", "eqp_model": "",
                "rows": 0, "n_lots": 0,
            })
            g["split"].append(m["split"])
            g["n_lots"] += m.get("n_lots", 0)
            g["rows"] += m.get("n_wafers", 0)
        for g in by_ppid.values():
            g["split"] = ", ".join(sorted(set(g["split"])))
            rows.append(g)
        return rows

    # ── ack (S3 ↔ 로컬 캐시) ──
    def load_ack(self) -> dict:
        text = None
        try:
            text = self.s3.get_text(self._ack_key())
        except Exception:
            pass
        if text is None and self._ack_cache().exists():
            text = self._ack_cache().read_text(encoding="utf-8")
        try:
            return json.loads(text) if text else {}
        except Exception:
            return {}

    def set_ack(self, alert_id: str, status: str, note: str = "", by: str = "valve") -> dict:
        ack = self.load_ack()
        if status and status != "active":
            ack[alert_id] = {"status": status, "note": note, "by": by, "ts": time.time()}
        else:
            ack.pop(alert_id, None)
        text = json.dumps(ack, ensure_ascii=False, indent=2)
        self._ack_cache().parent.mkdir(parents=True, exist_ok=True)
        self._ack_cache().write_text(text, encoding="utf-8")
        try:
            self.s3.put_text(self._ack_key(), text)
        except Exception:
            pass
        return ack

    # ── 통합 조회 + 발행 ──
    def list_alerts(self) -> dict:
        """모든 vehicle 알람 + ack 상태 병합. suppressed 도 status 만 달고 포함."""
        ack = self.load_ack()
        alerts = []
        for v in self.pipe.vehicles():
            alerts.extend(self.build(v))
        for a in alerts:
            a["status"] = (ack.get(a["id"]) or {}).get("status") or "active"
            a["note"] = (ack.get(a["id"]) or {}).get("note") or ""
        alerts.sort(key=lambda a: (a["status"] != "active", a["type"], a["vehicle"], a["step_id"]))
        active = sum(1 for a in alerts if a["status"] == "active")
        return {"alerts": alerts, "active": active,
                "suppressed": len(alerts) - active, "ack_key": self._ack_key()}

    # ── 발행 스냅샷 메타 (직전 발행 = 상태. event DB 갱신/재알람 판단 근거) ──
    def _pub_meta_path(self, vehicle: str) -> Path:
        return self.pipe.report_dir(vehicle) / "alerts_published.json"

    def load_pub_meta(self, vehicle: str) -> dict:
        p = self._pub_meta_path(vehicle)
        if p.exists():
            try:
                return json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                pass
        return {}

    def publish(self, vehicle: str):
        """활성 알람을 S3 로 발행 + 발행 스냅샷을 메타로 저장.
        직전 스냅샷과 비교해 first_seen 계승 + delta(new/resolved) 계산 —
        룰북/매칭테이블은 flow 가 버전관리하고, Valve 는 이 메타로
        'event DB 갱신 시 무엇이 새로/해소됐는지'를 참고한다."""
        ack = self.load_ack()
        cur = [a for a in self.build(vehicle)
               if (ack.get(a["id"]) or {}).get("status") not in SUPPRESS_STATUSES]

        prev = self.load_pub_meta(vehicle)
        prev_by_id = {a["id"]: a for a in prev.get("alerts", [])}
        now = time.time()
        for a in cur:
            a["first_seen_ts"] = (prev_by_id.get(a["id"]) or {}).get("first_seen_ts", now)
            a["last_seen_ts"] = now
        cur_ids = {a["id"] for a in cur}
        prev_ids = set(prev_by_id)
        payload = {
            "vehicle": vehicle, "ts": now, "count": len(cur),
            "delta": {"new": sorted(cur_ids - prev_ids), "resolved": sorted(prev_ids - cur_ids)},
            "alerts": cur,
        }
        text = json.dumps(payload, ensure_ascii=False, indent=2)

        # 로컬 메타 저장 (다음 발행의 기준 + event DB 갱신 참고). db/REPORTS/{vehicle}/
        try:
            mp = self._pub_meta_path(vehicle)
            mp.parent.mkdir(parents=True, exist_ok=True)
            mp.write_text(text, encoding="utf-8")
        except Exception:
            pass

        # S3 발행 — flow 가 읽어가는 지점
        try:
            ok = self.s3.put_text(f"{self.prefix}/pipeline/{vehicle}.json", text)
        except Exception:
            ok = False
        return payload if ok else False
