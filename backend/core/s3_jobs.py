"""
Valve · s3_jobs
---------------
S3 업/다운로드 전송 항목(item) 엔진 — flow 의 s3_ingest 와 같은 사용 모델.

항목 하나 = 전송 하나. "로컬 어디" ↔ "S3 어느 key" 를 짝지어 두고,
방향(download/upload) · 명령(sync/cp) · 주기를 각각 준다.

  config/s3_jobs.yaml
    items:
      - id: matching_vehicle
        direction: download          # download(S3→Valve) | upload(Valve→S3)
        root: config                 # 로컬 root — config | staging | db | outbox
        target: step_matching/vehicle_matching.csv   # root 기준 상대경로 (파일/폴더)
        dest: default                # S3 연결 이름 (s3_transfer.yaml 의 destinations)
        key: flow/artifacts/matching/Vehicle_matching.csv   # S3 key 또는 prefix
        mode: sync                   # sync(변경분만) | cp(항상 덮어쓰기)
        interval_min: 30             # 0 = 수동 전용
        enabled: true
    auto_download_enabled: true
    auto_upload_enabled: true

실행 모델 (flow 와 동일):
  · 단일 워커 큐 — 동시에 한 항목만. 나머지는 대기(queued).
  · 수동 실행 run(id) / 중지 stop(id). 중지는 파일 사이에서 취소 플래그를 확인하므로
    이미 옮긴 파일은 남고 나머지가 멈춘다 (반쪽 파일은 없음 — 원자적 저장).
  · 진행률은 폴링 — {done, total, current}. SSE 없음.
  · 상태 logs/s3_jobs_status.json · 이력 logs/s3_jobs_history.jsonl (최근 500).

Valve 의 S3 계층은 aws CLI 가 아니라 S3Uploader(boto3 / fake_local) 다.
그래서 파일 단위로 돌며 중간에 끊을 수 있다.
"""
from __future__ import annotations

import json
import re
import threading
import time
from pathlib import Path

import yaml

ID_RE = re.compile(r"^[A-Za-z0-9_\-]{1,64}$")
DIRECTIONS = ("download", "upload")
MODES = ("sync", "cp")
MAX_HISTORY = 500
TEXT_SUFFIXES = {".csv", ".yaml", ".yml", ".json", ".txt", ".md", ".py"}

_lock = threading.Lock()
_cond = threading.Condition(_lock)
_running: dict[str, dict] = {}     # id → {started, cancel, progress, thread}
_queued: list[str] = []
_worker: threading.Thread | None = None


class S3Jobs:
    """항목 CRUD + 실행 큐 + 상태/이력. app.py 가 하나만 만들어 라우터에 주입한다."""

    def __init__(self, root: Path, uploader_for, roots: dict[str, Path],
                 on_downloaded=None):
        self.root = Path(root)
        self.uploader_for = uploader_for      # dest 이름 → S3Uploader
        self.roots = roots                    # root 이름 → 로컬 절대경로
        self.on_downloaded = on_downloaded    # Callable[[list[str]], None] — 받은 로컬 경로들
        self._cfg_path = self.root / "config" / "s3_jobs.yaml"
        self._status_path = self.root / "logs" / "s3_jobs_status.json"
        self._history_path = self.root / "logs" / "s3_jobs_history.jsonl"
        self._sched_task = None

    # ── config ────────────────────────────────────────────
    def load(self) -> dict:
        if not self._cfg_path.exists():
            return {"items": [], "auto_download_enabled": True, "auto_upload_enabled": True}
        try:
            cfg = yaml.safe_load(self._cfg_path.read_text(encoding="utf-8")) or {}
        except Exception:
            return {"items": [], "auto_download_enabled": True, "auto_upload_enabled": True}
        cfg.setdefault("items", [])
        cfg.setdefault("auto_download_enabled", True)
        cfg.setdefault("auto_upload_enabled", True)
        return cfg

    def save(self, cfg: dict):
        self._cfg_path.parent.mkdir(parents=True, exist_ok=True)
        self._cfg_path.write_text(
            yaml.safe_dump(cfg, allow_unicode=True, sort_keys=False), encoding="utf-8")

    def items(self) -> list[dict]:
        return list(self.load().get("items") or [])

    def item(self, item_id: str) -> dict | None:
        return next((i for i in self.items() if i.get("id") == item_id), None)

    def validate(self, it: dict) -> dict:
        """항목 정규화 + 검증. 잘못된 값은 ValueError."""
        iid = str(it.get("id") or "").strip()
        if not ID_RE.match(iid):
            raise ValueError("id 는 영문/숫자/_/- 1~64자")
        direction = str(it.get("direction") or "download")
        if direction not in DIRECTIONS:
            raise ValueError(f"direction 은 {DIRECTIONS}")
        root = str(it.get("root") or "config")
        if root not in self.roots:
            raise ValueError(f"root 는 {tuple(self.roots)}")
        mode = str(it.get("mode") or "sync")
        if mode not in MODES:
            raise ValueError(f"mode 는 {MODES}")
        key = str(it.get("key") or "").strip().lstrip("/")
        if not key:
            raise ValueError("S3 key 가 비어있음")
        target = str(it.get("target") or "").strip().replace("\\", "/").strip("/")
        self._resolve(root, target)       # 경로 탈출 검증
        try:
            interval = max(0, int(it.get("interval_min") or 0))
        except (TypeError, ValueError):
            raise ValueError("interval_min 은 숫자")
        return {"id": iid, "direction": direction, "root": root, "target": target,
                "dest": str(it.get("dest") or "default").strip() or "default",
                "key": key, "mode": mode, "interval_min": interval,
                "enabled": bool(it.get("enabled", True)),
                "note": str(it.get("note") or "")[:200]}

    def upsert(self, it: dict) -> dict:
        norm = self.validate(it)
        cfg = self.load()
        items = [i for i in cfg["items"] if i.get("id") != norm["id"]]
        items.append(norm)
        cfg["items"] = sorted(items, key=lambda x: (x["direction"], x["id"]))
        self.save(cfg)
        return norm

    def delete(self, item_id: str) -> bool:
        cfg = self.load()
        before = len(cfg["items"])
        cfg["items"] = [i for i in cfg["items"] if i.get("id") != item_id]
        self.save(cfg)
        return len(cfg["items"]) != before

    def set_auto(self, download: bool | None = None, upload: bool | None = None) -> dict:
        cfg = self.load()
        if download is not None:
            cfg["auto_download_enabled"] = bool(download)
        if upload is not None:
            cfg["auto_upload_enabled"] = bool(upload)
        self.save(cfg)
        return cfg

    def _resolve(self, root: str, target: str) -> Path:
        base = Path(self.roots[root]).resolve()
        p = (base / target).resolve() if target else base
        try:
            p.relative_to(base)
        except ValueError:
            raise ValueError("target 이 root 밖을 가리킴")
        return p

    # ── status / history ──────────────────────────────────
    def status_all(self) -> dict:
        if not self._status_path.exists():
            return {}
        try:
            return json.loads(self._status_path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _write_status(self, item_id: str, patch: dict):
        st = self.status_all()
        st.setdefault(item_id, {}).update(patch)
        self._status_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._status_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(st, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self._status_path)

    def _append_history(self, rec: dict):
        self._history_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(self._history_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
            lines = self._history_path.read_text(encoding="utf-8").splitlines()
            if len(lines) > MAX_HISTORY:
                self._history_path.write_text(
                    "\n".join(lines[-MAX_HISTORY:]) + "\n", encoding="utf-8")
        except Exception:
            pass

    def history(self, item_id: str = "", limit: int = 100) -> list[dict]:
        if not self._history_path.exists():
            return []
        out = []
        try:
            for line in self._history_path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                if not item_id or r.get("id") == item_id:
                    out.append(r)
        except Exception:
            return []
        return out[-max(1, int(limit)):][::-1]

    # ── 조회 (UI 용) ───────────────────────────────────────
    def list_with_status(self) -> dict:
        cfg = self.load()
        st = self.status_all()
        now = time.time()
        out = []
        with _lock:
            running = {k: dict(v.get("progress") or {}) for k, v in _running.items()}
            queued = list(_queued)
        for it in cfg["items"]:
            s = st.get(it["id"], {})
            last = s.get("last_start")
            interval = float(it.get("interval_min") or 0) * 60
            auto_on = cfg["auto_download_enabled"] if it["direction"] == "download" \
                else cfg["auto_upload_enabled"]
            sched = bool(it.get("enabled") and interval > 0 and auto_on)
            next_due = (last + interval) if (last and interval > 0) else (now if sched else None)
            out.append({**it, "status": s,
                        "is_running": it["id"] in running,
                        "is_queued": it["id"] in queued,
                        "progress": running.get(it["id"]),
                        "scheduled": sched,
                        "next_due": next_due,
                        "due": bool(sched and (last is None or now - last >= interval))})
        return {"items": out, "auto_download_enabled": cfg["auto_download_enabled"],
                "auto_upload_enabled": cfg["auto_upload_enabled"],
                "roots": sorted(self.roots), "running": list(running), "queued": queued}

    def browse_keys(self, dest: str, prefix: str = "", limit: int = 300) -> dict:
        """S3 key 고르기 — prefix 아래 object 목록. '어떤 key 를 쓸지' 폼에서 사용."""
        up = self.uploader_for(dest)
        keys = up.list_objects(prefix or "")
        keys = sorted(keys)[:max(1, int(limit))]
        # 바로 아래 단계 폴더도 뽑아 준다 (탐색을 계단식으로)
        base = (prefix or "").rstrip("/")
        head = f"{base}/" if base else ""
        folders = sorted({k[len(head):].split("/", 1)[0]
                          for k in keys if k.startswith(head) and "/" in k[len(head):]})
        return {"dest": dest, "prefix": prefix, "keys": keys,
                "folders": [f"{head}{f}" for f in folders]}

    # ── 실행 ──────────────────────────────────────────────
    def run(self, item_id: str) -> dict:
        it = self.item(item_id)
        if not it:
            raise ValueError(f"알 수 없는 항목: {item_id}")
        with _cond:
            if item_id in _running:
                return {"ok": False, "already": "running"}
            if item_id in _queued:
                return {"ok": False, "already": "queued"}
            _queued.append(item_id)
            _cond.notify_all()
        self._ensure_worker()
        return {"ok": True, "queued": True}

    def stop(self, item_id: str) -> dict:
        """대기 중이면 큐에서 빼고, 실행 중이면 취소 플래그 — 다음 파일 경계에서 멈춘다."""
        with _cond:
            dequeued = item_id in _queued
            if dequeued:
                _queued.remove(item_id)
            slot = _running.get(item_id)
            if slot is not None:
                slot["cancel"] = True
            _cond.notify_all()
        return {"ok": True, "dequeued": dequeued, "cancelling": slot is not None}

    def _ensure_worker(self):
        global _worker
        with _lock:
            if _worker is not None and _worker.is_alive():
                return
            _worker = threading.Thread(target=self._worker_loop, name="s3-jobs", daemon=True)
            _worker.start()

    def _worker_loop(self):
        while True:
            with _cond:
                while not _queued:
                    if not _cond.wait(timeout=30):
                        if not _queued:
                            return          # 할 일 없으면 워커 종료 (run 이 다시 띄운다)
                item_id = _queued.pop(0)
                _running[item_id] = {"started": time.time(), "cancel": False,
                                     "progress": {"done": 0, "total": 0, "current": ""}}
            try:
                self._execute(item_id)
            except Exception as e:
                self._finish(item_id, "error", error=str(e)[:400])
            finally:
                with _cond:
                    _running.pop(item_id, None)

    def _progress(self, item_id: str, **kw) -> bool:
        """진행률 갱신 + 취소 여부 반환 (True = 계속)."""
        with _lock:
            slot = _running.get(item_id)
            if slot is None:
                return False
            slot["progress"].update(kw)
            return not slot.get("cancel")

    def _finish(self, item_id: str, status: str, **extra):
        now = time.time()
        st = self.status_all().get(item_id, {})
        rec = {"id": item_id, "ts": now, "status": status,
               "duration_sec": round(now - (st.get("last_start") or now), 2), **extra}
        self._write_status(item_id, {"last_end": now, "last_status": status, **extra})
        self._append_history(rec)
        return rec

    # ── 실제 전송 ──────────────────────────────────────────
    def _execute(self, item_id: str):
        it = self.item(item_id)
        if not it:
            raise ValueError(f"항목이 사라짐: {item_id}")
        self._write_status(item_id, {"last_start": time.time(), "last_status": "running",
                                     "direction": it["direction"]})
        up = self.uploader_for(it["dest"])
        fn = self._do_upload if it["direction"] == "upload" else self._do_download
        moved, skipped, failed, cancelled, touched = fn(item_id, it, up)
        status = "cancelled" if cancelled else ("error" if failed else "ok")
        self._finish(item_id, status, moved=moved, skipped=skipped, failed=failed,
                     direction=it["direction"])
        if touched and it["direction"] == "download" and self.on_downloaded:
            try:
                self.on_downloaded(touched)
            except Exception:
                pass

    @staticmethod
    def _is_text(p: Path) -> bool:
        return p.suffix.lower() in TEXT_SUFFIXES

    def _do_upload(self, item_id: str, it: dict, up):
        """로컬 target(파일/폴더) → S3 {key}/… . sync 는 변경분만."""
        base = self._resolve(it["root"], it["target"])
        if not base.exists():
            raise ValueError(f"로컬 경로 없음: {it['root']}/{it['target']}")
        if base.is_file():
            files = [("", base)]
        else:
            files = [(str(p.relative_to(base)).replace("\\", "/"), p)
                     for p in sorted(base.rglob("*")) if p.is_file()]
        moved = skipped = failed = 0
        cancelled = False
        self._progress(item_id, total=len(files), done=0)
        for i, (rel, p) in enumerate(files):
            if not self._progress(item_id, done=i, current=rel or p.name):
                cancelled = True
                break
            key = f"{it['key'].rstrip('/')}/{rel}" if rel else it["key"]
            if it["mode"] == "sync" and self._same(up, key, p):
                skipped += 1
                continue
            if up.put_file(key, p):
                moved += 1
            else:
                failed += 1
        self._progress(item_id, done=len(files) if not cancelled else None)
        return moved, skipped, failed, cancelled, []

    def _do_download(self, item_id: str, it: dict, up):
        """S3 {key}(또는 prefix) → 로컬 target. 단일 파일이면 target 이 그 파일."""
        keys = [it["key"]]
        single = up.head(it["key"]) is not None
        if not single:                       # prefix 로 간주하고 목록 조회
            keys = sorted(up.list_objects(it["key"]))
            if not keys:
                raise ValueError(f"S3 에 없음: {it['key']}")
        moved = skipped = failed = 0
        cancelled = False
        touched: list[str] = []
        self._progress(item_id, total=len(keys), done=0)
        head = it["key"].rstrip("/") + "/"
        for i, k in enumerate(keys):
            if not self._progress(item_id, done=i, current=k):
                cancelled = True
                break
            dst = (self._resolve(it["root"], it["target"]) if single
                   else self._resolve(it["root"], f"{it['target']}/{k[len(head):]}"
                                      if k.startswith(head) else
                                      f"{it['target']}/{k.rsplit('/', 1)[-1]}"))
            if it["mode"] == "sync" and dst.exists() and self._same(up, k, dst):
                skipped += 1
                continue
            if up.get_file(k, dst):
                moved += 1
                touched.append(str(dst))
            else:
                failed += 1
        self._progress(item_id, done=len(keys) if not cancelled else None)
        return moved, skipped, failed, cancelled, touched

    def _same(self, up, key: str, local: Path) -> bool:
        """sync 판정 — 텍스트는 내용, 바이너리는 크기 (탐색기 s3-transfer 와 동일 규칙)."""
        try:
            if self._is_text(local):
                remote = up.get_text(key)
                return remote is not None and remote == local.read_text(encoding="utf-8")
            head = up.head(key)
            return head is not None and head.get("size") == local.stat().st_size
        except Exception:
            return False

    # ── 주기 스케줄러 ───────────────────────────────────────
    async def _loop(self):
        import asyncio
        while True:
            try:
                for it in self.list_with_status()["items"]:
                    if it["due"] and not it["is_running"] and not it["is_queued"]:
                        self.run(it["id"])
            except Exception:
                pass
            await asyncio.sleep(30)

    def start_background(self):
        import asyncio
        if self._sched_task is None or self._sched_task.done():
            self._sched_task = asyncio.get_event_loop().create_task(self._loop())

    def stop_background(self):
        if self._sched_task and not self._sched_task.done():
            self._sched_task.cancel()
        self._sched_task = None

    # ── csv_sync.yaml / s3_transfer.yaml → 항목 1회 이관 ────
    def migrate_if_empty(self, csv_sync=None, transfer_rules: dict | None = None) -> int:
        """s3_jobs.yaml 이 없을 때만, 기존 설정을 항목으로 옮긴다. 만든 항목 수 반환."""
        if self._cfg_path.exists():
            return 0
        items: list[dict] = []
        if csv_sync is not None:
            try:
                cfg = csv_sync.load_config()
                for i, f in enumerate(cfg.get("files") or []):
                    dest_rel = str(f.get("dest") or "").replace("\\", "/")
                    root = "config" if dest_rel.startswith("config/") else "config"
                    target = dest_rel[len("config/"):] if dest_rel.startswith("config/") else dest_rel
                    # id 는 target 전체 경로로 만든다 — stem 만 쓰면
                    # fab_scan/VH_PRODA/scan_ignore.json 과 VH_PRODB/… 가 충돌한다
                    slug = re.sub(r"[^A-Za-z0-9_-]", "_",
                                  str(Path(target).with_suffix(""))) or f"dl{i}"
                    items.append({
                        "id": f"dl_{slug}"[:64],
                        "direction": "download", "root": root, "target": target,
                        "dest": "default", "key": csv_sync.full_key(cfg, f["key"]),
                        "mode": "sync",
                        "interval_min": int(cfg.get("interval_min") or 30)
                        if cfg.get("enabled") else 0,
                        "enabled": bool(cfg.get("enabled")),
                        "note": "csv_sync.yaml 에서 이관 (flow → Valve 매칭 csv)"})
            except Exception:
                pass
        for root, rule in (transfer_rules or {}).items():
            for t in (rule.get("targets") or [])[:1]:
                items.append({
                    "id": f"up_{root}"[:64], "direction": "upload", "root": root,
                    "target": "", "dest": t.get("dest", "default"),
                    "key": t.get("prefix", f"valve-export/{root}"),
                    "mode": rule.get("mode", "sync"), "interval_min": 0, "enabled": False,
                    "note": "s3_transfer.yaml 에서 이관 — 수동 전용으로 시작"})
        seen: dict[str, int] = {}
        for it in items:                       # id 중복이면 뒤에 번호를 붙인다
            n = seen.get(it["id"], 0)
            seen[it["id"]] = n + 1
            if n:
                it["id"] = f"{it['id'][:60]}_{n + 1}"
        self.save({"items": items, "auto_download_enabled": True,
                   "auto_upload_enabled": True})
        return len(items)
