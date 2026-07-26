"""
Valve · s3_link
---------------
탐색기 신호등 — 각 root/파일이 S3 와 어떤 방향(다운로드/업로드)으로 연동됐는지,
현재 상태(ok/pending/error/idle)를 판정한다.

실제 연동 근거:
  · config             : 파일마다 방향이 다르다 (annotate 의 config 분기 참조)
                         ↓ csv_sync 대상 · ↕ config_sync 대상 · 표시 없음 = 로컬 전용
  · staging ↑ 업로드   : executor 가 staging parquet 을 S3 로 push (SOURCE/product/date)
                         대기 중이면 s3_queue.pending() 에 있음
  · s3_local           : (fake) S3 저장소 그 자체 (cloud)
  · outbox  ↑ 업로드   : 알람 발행 미러 (S3 key 트리 1:1) — 외부 `aws s3 sync` 대상
  · db                 : 파이프라인 로컬 산출물 (직접 S3 연동 없음 — flow 가 공유 FS/S3 소비)

반환 syncinfo = {"dir": "down"|"up"|"both"|None,
                 "state": "ok|pending|error|idle|cloud|local", "detail": str}
  dir   → 화살표 (down=↓ 파랑, up=↑ 초록, both=↕ 보라, None=화살표 없음)
          방향이 색으로 갈린다 — 받는 파일과 올리는 파일을 한눈에 구분하기 위함.
  state → 신호등 색 (ok=green, pending=amber, error=red, idle=gray, cloud=blue,
                     local=연동 없음)
"""
from __future__ import annotations

from pathlib import Path

from backend.core.config_sync import BOOT_PULL_FILES


def _norm(p) -> str:
    return str(p).replace("\\", "/")


def build_annotator(csv_sync=None, s3=None, s3queue=None):
    """root 별로 (rel, is_dir, abspath) 항목들을 받아 {rel: syncinfo} 를 돌려주는 함수."""

    def _s3_active() -> bool:
        if s3 is None:
            return False
        try:
            return s3._is_fake() or s3._s3_client is not None
        except Exception:
            return bool(getattr(s3, "bucket", None))

    def annotate(root: str, items: list[tuple]) -> dict[str, dict]:
        out: dict[str, dict] = {}

        if root == "config":
            # config 루트는 세 종류가 섞여 있다 — 방향을 파일 단위로 갈라준다.
            #   ↓ down  csv_sync 대상 (Vehicle_matching · ppid_knob · inline_matching …)
            #           flow 가 소유하고 Valve 는 받기만 한다. 올리면 다음 sync 에 덮인다.
            #   ↕ both  config_sync 대상 (settings/products/source_types)
            #           기동 시 pull · 탐색기에서 push(seed) 도 가능
            #   ·  local 그 외 (pipeline.yaml · vehicles.yaml · feature_funcs.py …) — S3 미연동
            #
            # csv_sync 는 dest(=Valve 로컬 경로) 기준으로 매칭한다. 설정에만 있고 아직
            # 한 번도 안 받은 파일도 down 으로 잡히도록 status 가 아니라 config 를 본다.
            dest_status: dict[str, dict] = {}
            dest_keys: dict[str, str] = {}

            def _strip(dest: str) -> str:
                dest = _norm(dest)
                return dest[len("config/"):] if dest.startswith("config/") else dest

            if csv_sync is not None:
                try:
                    cfg = csv_sync.load_config()
                    for f in cfg.get("files", []):
                        dest_keys[_strip(f.get("dest") or "")] = csv_sync.full_key(cfg, f["key"])
                except Exception:
                    pass
                try:
                    for e in csv_sync.load_status().values():
                        dest_status[_strip(e.get("dest") or "")] = e
                except Exception:
                    pass

            # missing = flow 가 아직 안 올림 → 오류(빨강)가 아니라 대기(주황)
            state_map = {"updated": "ok", "unchanged": "ok",
                         "missing": "pending", "error": "error"}
            pull_dirs = {p.rsplit("/", 1)[0] for p in dest_keys if "/" in p}

            for rel, is_dir, _abs in items:
                key = _norm(rel)
                if key in dest_keys:
                    e = dest_status.get(key)
                    st = (e or {}).get("status")
                    detail = f"flow 가 관리 → Valve 는 받기만 함\nS3 key: {dest_keys[key]}"
                    if st:
                        detail += f"\n마지막 동기화: {st}"
                    else:
                        detail += "\n아직 동기화 이력 없음"
                    out[rel] = {"dir": "down", "state": state_map.get(st, "idle"),
                                "detail": detail}
                elif not is_dir and key in BOOT_PULL_FILES:
                    out[rel] = {"dir": "both", "state": "ok",
                                "detail": "기동 시 S3(valve-config) 에서 pull · 탐색기에서 push 가능"}
                elif is_dir:
                    down = key in pull_dirs or any(p.startswith(key + "/") for p in dest_keys)
                    out[rel] = ({"dir": "down", "state": "idle", "detail": "flow → Valve 다운로드 폴더"}
                                if down else
                                {"dir": None, "state": "local", "detail": "로컬 전용 설정 폴더"})
                else:
                    out[rel] = {"dir": None, "state": "local",
                                "detail": "로컬 전용 설정 (S3 자동 동기화 없음)"}
            return out

        if root == "staging":
            active = _s3_active()
            pending_abs = set()
            if s3queue is not None:
                try:
                    pending_abs = {_norm(Path(q["local_path"]).resolve()) for q in s3queue.pending()}
                except Exception:
                    pending_abs = set()
            for rel, is_dir, _abs in items:
                if not active:
                    out[rel] = {"dir": "up", "state": "idle", "detail": "S3 미설정"}
                elif (not is_dir) and _norm(Path(_abs).resolve()) in pending_abs:
                    out[rel] = {"dir": "up", "state": "pending", "detail": "S3 업로드 대기 큐"}
                else:
                    out[rel] = {"dir": "up", "state": "ok",
                                "detail": "S3 업로드 대상 (SOURCE/product/date)"}
            return out

        if root == "s3_local":
            for rel, is_dir, _abs in items:
                out[rel] = {"dir": None, "state": "cloud", "detail": "S3 (fake) 저장소"}
            return out

        if root == "db":
            for rel, is_dir, _abs in items:
                out[rel] = {"dir": None, "state": "idle", "detail": "파이프라인 로컬 산출물"}
            return out

        if root == "outbox":
            # 알람 발행 미러 — 루트가 {alerts.s3_prefix} 이고 그 아래는 S3 key 와 1:1.
            for rel, is_dir, _abs in items:
                out[rel] = {"dir": "up", "state": "ok",
                            "detail": "S3 업로드 폴더 — prefix 아래 경로가 곧 S3 key"}
            return out

        return out

    return annotate


def root_role(root: str) -> dict:
    """Roots 목록에 붙일 방향/설명."""
    return {
        "config": {"dir": "both", "detail": "파일별로 다름 — ↓ 매칭 csv(flow 소유) · ↕ settings/products · 나머지 로컬"},
        "staging": {"dir": "up", "detail": "Valve → S3 업로드 (추출 산출)"},
        "outbox": {"dir": "up", "detail": "알람 업로드 폴더 — 이 폴더만 S3 sync (flow 매칭알람)"},
        "s3_local": {"dir": None, "detail": "S3 (fake) 저장소"},
        "db": {"dir": None, "detail": "파이프라인 로컬 산출물 (raw/event/feature)"},
    }.get(root, {"dir": None, "detail": ""})
