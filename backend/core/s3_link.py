"""
Valve · s3_link
---------------
탐색기 신호등 — 각 root/파일이 S3 와 어떤 방향(다운로드/업로드)으로 연동됐는지,
현재 상태(ok/pending/error/idle)를 판정한다.

실제 연동 근거:
  · config  ↓ 다운로드 : csv_sync 가 S3 → 로컬 config 로 pull (logs/csv_sync.json 상태)
  · staging ↑ 업로드   : executor 가 staging parquet 을 S3 로 push (SOURCE/product/date)
                         대기 중이면 s3_queue.pending() 에 있음
  · s3_local           : (fake) S3 저장소 그 자체 (cloud)
  · db                 : 파이프라인 로컬 산출물 (직접 S3 연동 없음 — flow 가 공유 FS/S3 소비)

반환 syncinfo = {"dir": "down"|"up"|None, "state": "ok|pending|error|idle|cloud", "detail": str}
  dir   → 화살표 방향 (down=↓, up=↑, None=화살표 없음)
  state → 신호등 색 (ok=green, pending=amber, error=red, idle=gray, cloud=blue)
"""
from __future__ import annotations

from pathlib import Path


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
            # csv_sync dest(‘config/…’) → 상태. 없는 파일은 로컬 전용.
            dest_status: dict[str, dict] = {}
            if csv_sync is not None:
                try:
                    for e in csv_sync.load_status().values():
                        dest = _norm(e.get("dest") or "")
                        if dest.startswith("config/"):
                            dest = dest[len("config/"):]
                        dest_status[dest] = e
                except Exception:
                    pass
            state_map = {"updated": "ok", "unchanged": "ok", "missing": "error", "error": "error"}
            for rel, is_dir, _abs in items:
                e = dest_status.get(_norm(rel))
                if e:
                    st = e.get("status")
                    out[rel] = {"dir": "down", "state": state_map.get(st, "idle"),
                                "detail": f"csv_sync {st} · {e.get('s3_key', '')}"}
                elif is_dir:
                    out[rel] = {"dir": "down", "state": "idle", "detail": "설정 pull 루트 (flow→Valve)"}
                else:
                    out[rel] = {"dir": "down", "state": "idle", "detail": "로컬 설정 (S3 미동기화)"}
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

        return out

    return annotate


def root_role(root: str) -> dict:
    """Roots 목록에 붙일 방향/설명."""
    return {
        "config": {"dir": "down", "detail": "flow → Valve 설정 pull"},
        "staging": {"dir": "up", "detail": "Valve → S3 업로드 (추출 산출)"},
        "s3_local": {"dir": None, "detail": "S3 (fake) 저장소"},
        "db": {"dir": None, "detail": "파이프라인 로컬 산출물 (raw/event/feature)"},
    }.get(root, {"dir": None, "detail": ""})
