"""
Valve · csv_sync
----------------
flow 가 S3 에 올린 csv 설정파일(matching/*.csv)을 주기적으로 내려받아 로컬 config 에 반영.
(flow 쪽 업로더: flow/backend/core/s3_sync.py — key 규약 `{prefix}/matching/{name}.csv`)

설정: config/csv_sync.yaml
  enabled: true
  interval_min: 30                # 다운로드 주기 (분)
  s3_prefix: flow/artifacts       # flow 업로드 prefix
  files:
    - key: matching/step_matching.csv           # S3 key ({s3_prefix}/ 이하)
      dest: config/step_matching/vehicle_matching.csv   # Valve 로컬 경로

상태: logs/csv_sync.json — 파일별 {status, ts, sha1}. status:
  updated   내려받아 내용 변경
  unchanged 내용 동일 (쓰기 생략)
  missing   S3 에 key 없음
  error     읽기/쓰기 실패
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import time
from pathlib import Path

import yaml

DEFAULT_CONFIG = {
    "enabled": False,
    "interval_min": 30,
    "s3_prefix": "flow/artifacts",
    "files": [],
}


class CsvSync:
    def __init__(self, root: Path, s3_uploader):
        self.root = Path(root)
        self.s3 = s3_uploader
        self._task: asyncio.Task | None = None
        # 파일이 실제로 갱신됐을 때 호출 — router 가 event/feature 재생성 훅으로 사용
        self.on_updated = None  # Callable[[list[str]], None] | None

    # ── config ──
    def _cfg_path(self) -> Path:
        return self.root / "config" / "csv_sync.yaml"

    def _status_path(self) -> Path:
        return self.root / "logs" / "csv_sync.json"

    def load_config(self) -> dict:
        fp = self._cfg_path()
        if not fp.exists():
            return dict(DEFAULT_CONFIG)
        cfg = dict(DEFAULT_CONFIG)
        cfg.update(yaml.safe_load(fp.read_text(encoding="utf-8")) or {})
        return cfg

    def save_config(self, cfg: dict):
        out = {
            "enabled": bool(cfg.get("enabled")),
            "interval_min": max(1, int(cfg.get("interval_min") or 30)),
            "s3_prefix": str(cfg.get("s3_prefix") or "").strip().strip("/"),
            "files": [
                {"key": str(f.get("key") or "").strip().lstrip("/"),
                 "dest": str(f.get("dest") or "").strip()}
                for f in (cfg.get("files") or [])
                if str(f.get("key") or "").strip() and str(f.get("dest") or "").strip()
            ],
        }
        self._cfg_path().write_text(
            yaml.safe_dump(out, allow_unicode=True, sort_keys=False), encoding="utf-8")
        return out

    # ── status ──
    def load_status(self) -> dict:
        fp = self._status_path()
        if not fp.exists():
            return {}
        try:
            return json.loads(fp.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _save_status(self, status: dict):
        fp = self._status_path()
        fp.parent.mkdir(parents=True, exist_ok=True)
        fp.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")

    # ── sync ──
    def full_key(self, cfg: dict, file_key: str) -> str:
        prefix = (cfg.get("s3_prefix") or "").strip("/")
        return f"{prefix}/{file_key}" if prefix else file_key

    def sync_now(self) -> list[dict]:
        """설정된 모든 파일 1회 동기화. 파일별 결과 리스트 반환 + 상태 저장."""
        cfg = self.load_config()
        status = self.load_status()
        results = []
        for f in cfg.get("files", []):
            key, dest = f["key"], f["dest"]
            entry = {"key": key, "s3_key": self.full_key(cfg, key), "dest": dest,
                     "ts": time.time()}
            try:
                text = self.s3.get_text(self.full_key(cfg, key))
                if text is None:
                    entry["status"] = "missing"
                else:
                    dest_path = self.root / dest
                    current = dest_path.read_text(encoding="utf-8") if dest_path.exists() else None
                    if current == text:
                        entry["status"] = "unchanged"
                    else:
                        dest_path.parent.mkdir(parents=True, exist_ok=True)
                        dest_path.write_text(text, encoding="utf-8")
                        entry["status"] = "updated"
                    entry["sha1"] = hashlib.sha1(text.encode("utf-8")).hexdigest()[:12]
            except Exception as e:
                entry["status"] = "error"
                entry["error"] = str(e)[:300]
            status[key] = entry
            results.append(entry)
        self._save_status(status)
        updated = [e["dest"] for e in results if e["status"] == "updated"]
        if updated and self.on_updated:
            try:
                self.on_updated(updated)
            except Exception:
                pass
        return results

    # ── background loop ──
    def start_background(self):
        if self._task is None or self._task.done():
            self._task = asyncio.get_event_loop().create_task(self._loop())

    def stop_background(self):
        if self._task and not self._task.done():
            self._task.cancel()
        self._task = None

    async def _loop(self):
        while True:
            cfg = self.load_config()
            if cfg.get("enabled"):
                try:
                    await asyncio.to_thread(self.sync_now)
                except Exception:
                    pass
            await asyncio.sleep(max(1, int(cfg.get("interval_min") or 30)) * 60)
