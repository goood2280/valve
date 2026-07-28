"""
Valve · alert_store
-------------------
파이프라인 알람(미매칭 step · KNOB RO ppid)의 통합 리스트 + flow 와의 S3 순환.

순환 구조:
  1. Valve 파이프라인 실행 → 알람 생성(전체 스캔) → S3 `{alerts_prefix}/pipeline/{vehicle}.json` 발행
     + 같은 내용을 **업로드 폴더**(alerts.outbox_dir, 기본 `s3_outbox/`)에 S3 key 와
       똑같은 트리로 미러링 — 이 폴더 하나만 `aws s3 sync` 하면 flow 매칭알람이 완성된다
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
ack.json 은 flow 도 쓰는 양방향 파일이라 업로드 폴더에 두지 않는다 — 폴더 sync 로
덮으면 flow 의 판정이 유실된다. (업로드 폴더 = Valve 단독 소유 파일만)
"""
from __future__ import annotations

import json
import time
from pathlib import Path

from backend.core.feature_pipeline import alert_scan_cols

SUPPRESS_STATUSES = ("미확인예정", "반영불필요")


class AlertStore:
    def __init__(self, pipe, s3_uploader, settings: dict, root: Path):
        self.pipe = pipe
        self.s3 = s3_uploader
        self.root = Path(root)
        # settings 참조 유지 — 설정 API 가 in-place merge 하므로 s3_prefix/s3_enabled
        # 변경이 재시작 없이 반영된다.
        self.settings = settings

    @property
    def prefix(self) -> str:
        return ((self.settings.get("alerts") or {}).get("s3_prefix") or "valve-alerts").strip("/")

    def _s3_enabled(self) -> bool:
        """alerts.s3_enabled — 알람 JSON/ack 의 S3 업로드·다운로드 사용 여부."""
        return bool((self.settings.get("alerts") or {}).get("s3_enabled", True))

    def alert_cols(self) -> list[str]:
        """⚙ 로 설정한 알람 전송 열 (pipeline.yaml unmatched_scan.alert_cols)."""
        try:
            return alert_scan_cols(self.pipe.global_cfg().get("unmatched_scan") or {})
        except Exception:
            return []

    def _ack_key(self) -> str:
        return f"{self.prefix}/pipeline/ack.json"

    def _ack_cache(self) -> Path:
        return self.root / "logs" / "alerts_ack.json"

    # ── 업로드 폴더 (S3 key 트리를 그대로 미러 — 외부 `aws s3 sync` 대상) ──
    @property
    def outbox_dir(self) -> str:
        """alerts.outbox_dir — ROOT 기준 상대(또는 절대) 경로. 빈 값이면 미러링 안 함."""
        return str((self.settings.get("alerts") or {}).get("outbox_dir", "s3_outbox")).strip()

    def outbox_root(self) -> Path | None:
        """업로드 폴더의 루트 = 버킷 루트에 대응. 없으면 None."""
        d = self.outbox_dir
        if not d:
            return None
        p = Path(d)
        return p if p.is_absolute() else (self.root / p)

    def outbox_sync_dir(self) -> Path | None:
        """실제 sync 단위 — 이 폴더가 s3://{bucket}/{s3_prefix} 에 1:1 대응."""
        root = self.outbox_root()
        return (root / self.prefix) if root else None

    def _outbox_path(self, key: str) -> Path | None:
        root = self.outbox_root()
        return (root / key) if root else None

    def _outbox_write(self, key: str, text: str) -> bool:
        """S3 key 와 같은 상대경로로 원자적 저장. 내용이 같으면 mtime 도 안 건드림
        (aws s3 sync 가 불필요한 업로드를 하지 않도록).

        바이트로 쓴다 — write_text 는 Windows 에서 \\n 을 \\r\\n 으로 바꿔
        S3 직접 발행본(put_text=바이트)과 내용이 달라진다."""
        fp = self._outbox_path(key)
        if fp is None:
            return False
        data = text.encode("utf-8")
        try:
            if fp.exists() and fp.read_bytes() == data:
                return True
            fp.parent.mkdir(parents=True, exist_ok=True)
            tmp = fp.with_suffix(fp.suffix + ".tmp")
            tmp.write_bytes(data)
            tmp.replace(fp)
            return True
        except Exception:
            return False

    def outbox_status(self) -> dict:
        """업로드 폴더 현황 — UI/운영 점검용 (경로 · 파일 목록 · 발행 주기)."""
        root = self.outbox_root()
        key_prefix = f"{self.prefix}/pipeline"
        info = {
            "enabled": root is not None,
            "root": str(root) if root else "",
            "sync_dir": str(self.outbox_sync_dir() or ""),
            "s3_prefix": self.prefix,
            "key_prefix": key_prefix,
            "interval_min": self.interval_min(),
            "files": [],
        }
        if root is None:
            return info
        d = root / key_prefix
        if d.is_dir():
            for f in sorted(d.glob("*.json")):
                st = f.stat()
                info["files"].append({"key": f"{key_prefix}/{f.name}", "name": f.name,
                                      "size": st.st_size, "mtime": st.st_mtime})
        return info

    # ── 알람 생성 (vehicle 별 통합 행) ──
    def build(self, vehicle: str) -> list[dict]:
        rows: list[dict] = []
        try:
            unm = self.pipe.scan_unmatched(vehicle)
        except RuntimeError:
            unm = None
        if unm:
            extras = unm.get("step_extras") or {}
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
            for sid, g in by_step.items():
                g["eqp_id"] = ", ".join(sorted(filter(None, g["eqp_id"])))
                g["eqp_model"] = ", ".join(sorted(filter(None, g["eqp_model"])))
                # 알람 부가정보 — 예시 lot/wafer 쌍 + ⚙ 로 설정한 전송 열.
                # 기존 키(eqp 합산값 등)와 겹치는 열은 덮지 않는다.
                ext = extras.get(str(sid)) or {}
                g["examples"] = ext.get("examples") or []
                for c, val in (ext.get("cols") or {}).items():
                    if c not in g:
                        g[c] = val
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
        if self._s3_enabled():
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
        if self._s3_enabled():
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
                "suppressed": len(alerts) - active, "ack_key": self._ack_key(),
                "alert_cols": self.alert_cols()}

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
        """알람을 S3 로 발행 + 발행 스냅샷을 메타로 저장.

        억제(미확인예정/반영불필요) 건도 status 를 달아 포함한다 — flow 화면에서
        계속 보이고 나중에 되돌릴 수 있어야 하므로. count/delta 는 활성 건 기준.
        직전 스냅샷과 비교해 first_seen 계승 + delta(new/resolved) 계산 —
        룰북/매칭테이블은 flow 가 버전관리하고, Valve 는 이 메타로
        'event DB 갱신 시 무엇이 새로/해소됐는지'를 참고한다."""
        ack = self.load_ack()
        cur = self.build(vehicle)
        for a in cur:
            info = ack.get(a["id"]) or {}
            a["status"] = info.get("status") or "active"
            a["ack_note"] = info.get("note") or ""
        active_ids = {a["id"] for a in cur if a["status"] not in SUPPRESS_STATUSES}

        prev = self.load_pub_meta(vehicle)
        prev_by_id = {a["id"]: a for a in prev.get("alerts", [])}
        prev_active = {a["id"] for a in prev.get("alerts", [])
                       if (a.get("status") or "active") not in SUPPRESS_STATUSES}
        now = time.time()
        for a in cur:
            a["first_seen_ts"] = (prev_by_id.get(a["id"]) or {}).get("first_seen_ts", now)
            a["last_seen_ts"] = now
        payload = {
            "vehicle": vehicle, "ts": now, "count": len(active_ids),
            "suppressed": len(cur) - len(active_ids),
            "alert_cols": self.alert_cols(),   # flow 가 동적 열 렌더링에 사용
            "fp": self._fingerprint(cur, ack),
            "delta": {"new": sorted(active_ids - prev_active),
                      "resolved": sorted(prev_active - active_ids)},
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

        # 업로드 폴더 미러 — flow 가 읽어가는 지점의 1차 전송로.
        # 외부 스케줄러가 이 폴더만 `aws s3 sync` 하면 flow 매칭알람이 갱신된다.
        # s3_enabled 와 무관하게 항상 쓴다 (직접 S3 접근이 없는 환경의 유일한 경로).
        key = f"{self.prefix}/pipeline/{vehicle}.json"
        self._outbox_write(key, text)

        # S3 직접 발행 — 자격증명이 있는 환경의 보조 경로 (alerts.s3_enabled 로 on/off)
        if not self._s3_enabled():
            return False
        try:
            ok = self.s3.put_text(key, text)
        except Exception:
            ok = False
        return payload if ok else False

    # ── 주기 발행 (S3 업로드 스케줄러 — flow valve_alerts 폴러와 짝) ──
    @staticmethod
    def _fingerprint(cur: list[dict], ack: dict) -> str:
        """알람 id + ack 상태의 지문 — 주기 발행에서 '변경 없음' 판단 기준.
        (rows/n_lots 등 수치는 파이프라인 재실행 시 on_vehicle_done 발행이 갱신)"""
        import hashlib
        parts = sorted(
            f"{a.get('id')}|{(ack.get(a.get('id')) or {}).get('status') or 'active'}"
            for a in cur)
        return hashlib.sha1("\n".join(parts).encode("utf-8")).hexdigest()[:16]

    def publish_if_changed(self, vehicle: str) -> dict:
        """직전 발행 이후 알람 구성(id/ack 상태)이 바뀐 경우에만 S3 발행.

        업로드 폴더에 파일이 아직 없으면 지문이 같아도 발행한다 — 폴더를 지웠거나
        outbox 도입 전 스냅샷만 있는 경우에도 첫 사이클에 채워지도록."""
        ack = self.load_ack()
        cur = self.build(vehicle)
        fp = self._fingerprint(cur, ack)
        prev_fp = self.load_pub_meta(vehicle).get("fp")
        out = self._outbox_path(f"{self.prefix}/pipeline/{vehicle}.json")
        if prev_fp == fp and (out is None or out.exists()):
            return {"vehicle": vehicle, "skipped": True, "fp": fp}
        r = self.publish(vehicle)
        return {"vehicle": vehicle, "skipped": False, "published": bool(r), "fp": fp}

    def publish_all_if_changed(self) -> list[dict]:
        out = []
        for v in self.pipe.vehicles():
            try:
                out.append(self.publish_if_changed(v))
            except Exception as e:
                out.append({"vehicle": v, "error": str(e)[:200]})
        return out

    def interval_min(self) -> float:
        """alerts.s3_interval_min — 0 이면 주기 발행 안 함 (파이프라인 실행 시에만)."""
        try:
            return float((self.settings.get("alerts") or {}).get("s3_interval_min") or 0)
        except (TypeError, ValueError):
            return 0.0

    def start_background(self):
        """주기 발행 루프. 설정은 매 사이클 다시 읽음 — 재시작 없이 on/off/주기 변경 반영."""
        import asyncio
        if getattr(self, "_bg_task", None) is not None and not self._bg_task.done():
            return
        self._bg_task = asyncio.get_event_loop().create_task(self._publish_loop())

    def stop_background(self):
        task = getattr(self, "_bg_task", None)
        if task is not None and not task.done():
            task.cancel()
        self._bg_task = None

    async def _publish_loop(self):
        import asyncio
        while True:
            interval = self.interval_min()
            try:
                # 비활성(0)일 때도 60초마다 설정 재확인 — 켜면 다음 사이클부터 동작
                await asyncio.sleep(max(60.0, interval * 60) if interval > 0 else 60.0)
            except asyncio.CancelledError:
                return
            # 발행처가 하나도 없으면(직접 S3 off + 업로드 폴더 off) 돌 이유가 없다.
            if self.interval_min() <= 0 or not (self._s3_enabled() or self.outbox_dir):
                continue
            try:
                await asyncio.to_thread(self.publish_all_if_changed)
            except Exception:
                pass
