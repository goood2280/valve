"""browser 라우터 — staging · s3_local · config · db 파일탐색기 (flow FileBrowser 경량 버전)."""
from __future__ import annotations

import hashlib
from pathlib import Path

from fastapi import APIRouter, Body, HTTPException, Query

from backend.core import s3_link

router = APIRouter(prefix="/api/browser", tags=["browser"])

# config 루트에 노출/전송할 설정파일 확장자 (csv 는 표로, 나머지는 텍스트로 열람)
CONFIG_SUFFIXES = {".csv", ".yaml", ".yml", ".json", ".txt", ".md"}

_roots: dict[str, Path] = {}
_annotate = None       # s3 연동 신호등 어노테이터 (app.py 가 주입)
_s3 = None             # S3Uploader — s3 전송(put)
_csv_sync = None       # CsvSync — dest↔key 매핑으로 flow round-trip key 계산
_config_prefix = "valve-config"


def deps(staging_root: Path, s3_local_root: Path | None, extra_roots: dict[str, Path] | None = None,
         annotator=None, s3=None, csv_sync=None, config_prefix: str = "valve-config"):
    """extra_roots — 항상 노출할 추가 루트 (config: 설정파일, db: 파이프라인 산출물).
    annotator — 파일별 다운로드/업로드 신호등. s3/csv_sync — 파일별 S3 전송(업로드)용."""
    global _roots, _annotate, _s3, _csv_sync, _config_prefix
    _roots = {"staging": Path(staging_root)}
    if s3_local_root:
        _roots["s3_local"] = Path(s3_local_root)
    for name, p in (extra_roots or {}).items():
        _roots[name] = Path(p)
    _annotate = annotator
    _s3 = s3
    _csv_sync = csv_sync
    _config_prefix = (config_prefix or "valve-config").strip("/")


def s3_key_for(root: str, rel: str) -> str:
    """로컬 파일 → S3 key. config 이고 csv_sync dest 면 그 key(flow round-trip),
    아니면 {config_prefix|export}/{root}/{rel}."""
    rel = (rel or "").replace("\\", "/")
    if root == "config" and _csv_sync is not None:
        try:
            cfg = _csv_sync.load_config()
            for f in cfg.get("files", []):
                dest = (f.get("dest") or "").replace("\\", "/")
                if dest.startswith("config/"):
                    dest = dest[len("config/"):]
                if dest == rel:
                    return _csv_sync.full_key(cfg, f["key"])
        except Exception:
            pass
    base = _config_prefix if root == "config" else "valve-export"
    return f"{base}/{root}/{rel}" if root != "config" else f"{base}/{rel}"


def resolve(root: str, rel: str) -> Path:
    """root 이름 + 상대경로 → 검증된 절대경로 (query 라우터도 공유)."""
    base = _roots.get(root)
    if base is None:
        raise HTTPException(404, f"unknown root {root!r}")
    base = base.resolve()
    target = (base / rel).resolve() if rel else base
    try:
        target.relative_to(base)
    except ValueError:
        raise HTTPException(400, "path escape")
    return target


@router.get("/roots")
def list_roots():
    return {"roots": [{"name": n, "path": str(p), **s3_link.root_role(n)}
                      for n, p in _roots.items()
                      if n in ("staging", "config") or p.exists()]}


@router.get("/list")
def list_dir(root: str = Query(...), path: str = Query("")):
    base = _roots.get(root)
    if base is None:
        raise HTTPException(404, f"unknown root {root!r}")
    base.mkdir(parents=True, exist_ok=True)
    target = resolve(root, path)
    if not target.exists():
        return {"entries": [], "path": path}
    if not target.is_dir():
        raise HTTPException(400, "path is not a directory")

    entries = []
    for p in sorted(target.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower())):
        # config 루트는 설정파일(csv/yaml/json/…)만 노출 — 그 외 잡파일은 숨김
        if root == "config" and p.is_file() and p.suffix.lower() not in CONFIG_SUFFIXES:
            continue
        try:
            stat = p.stat()
            rel = f"{path}/{p.name}" if path else p.name
            entries.append({
                "name": p.name,
                "is_dir": p.is_dir(),
                "size": stat.st_size if not p.is_dir() else 0,
                "mtime": stat.st_mtime,
                "suffix": p.suffix,
                "_rel": rel,
                "_abs": str(p),
            })
        except Exception:
            continue

    # s3 연동 신호등 부착 (다운로드↓/업로드↑ · ok/pending/error/idle)
    if _annotate is not None:
        try:
            sync = _annotate(root, [(e["_rel"], e["is_dir"], e["_abs"]) for e in entries])
            for e in entries:
                e["sync"] = sync.get(e["_rel"])
        except Exception:
            pass
    for e in entries:
        e.pop("_rel", None)
        e.pop("_abs", None)
    return {"entries": entries, "path": path, "root": root}


@router.get("/config-files")
def config_files():
    """config 루트의 설정파일을 재귀적으로 평평하게 — Roots 최상위 빠른 목록 + S3 전송용.
    각 파일에 신호등(sync)과 목표 S3 key 를 함께 반환."""
    base = _roots.get("config")
    if base is None:
        return {"files": []}
    base = base.resolve()
    items = []
    for p in sorted(base.rglob("*"), key=lambda x: str(x).lower()):
        if not p.is_file() or p.suffix.lower() not in CONFIG_SUFFIXES:
            continue
        rel = str(p.relative_to(base)).replace("\\", "/")
        try:
            st = p.stat()
        except Exception:
            continue
        items.append({"rel": rel, "name": p.name, "suffix": p.suffix,
                      "size": st.st_size, "mtime": st.st_mtime,
                      "s3_key": s3_key_for("config", rel), "_abs": str(p)})
    if _annotate is not None:
        try:
            sync = _annotate("config", [(it["rel"], False, it["_abs"]) for it in items])
            for it in items:
                it["sync"] = sync.get(it["rel"])
        except Exception:
            pass
    for it in items:
        it.pop("_abs", None)
    return {"files": items}


@router.post("/s3-transfer")
def s3_transfer(body: dict = Body(...)):
    """로컬 설정파일을 S3 로 업로드. mode: cp(항상 덮어씀) | sync(내용 다를 때만).
    body: {root, path, mode}."""
    if _s3 is None:
        raise HTTPException(503, "s3 미설정")
    root = str(body.get("root") or "config")
    path = str(body.get("path") or "").strip()
    mode = str(body.get("mode") or "sync").lower()
    if mode not in ("cp", "sync"):
        raise HTTPException(400, "mode 는 cp | sync")
    target = resolve(root, path)
    if not target.exists() or target.is_dir():
        raise HTTPException(400, "파일이 아님")
    if target.suffix.lower() not in CONFIG_SUFFIXES:
        raise HTTPException(400, "설정파일(csv/yaml/json/txt/md)만 전송 지원")

    key = s3_key_for(root, path)
    local = target.read_text(encoding="utf-8")
    if mode == "sync":
        try:
            remote = _s3.get_text(key)
        except Exception:
            remote = None
        if remote is not None and remote == local:
            return {"status": "unchanged", "s3_key": key, "mode": mode,
                    "sha": hashlib.sha1(local.encode("utf-8")).hexdigest()[:12]}
    ok = _s3.put_text(key, local)
    return {"status": "uploaded" if ok else "error", "s3_key": key, "mode": mode,
            "sha": hashlib.sha1(local.encode("utf-8")).hexdigest()[:12]}
