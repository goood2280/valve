"""browser 라우터 — staging + s3_local 파일탐색기 (flow FileBrowser 경량 버전)."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/api/browser", tags=["browser"])

_staging_root: Path = None
_s3_local_root: Path = None


def deps(staging_root: Path, s3_local_root: Path | None):
    global _staging_root, _s3_local_root
    _staging_root = Path(staging_root)
    _s3_local_root = Path(s3_local_root) if s3_local_root else None


def _safe_resolve(root: Path, rel: str) -> Path:
    root = root.resolve()
    target = (root / rel).resolve() if rel else root
    try:
        target.relative_to(root)
    except ValueError:
        raise HTTPException(400, "path escape")
    return target


@router.get("/roots")
def list_roots():
    roots = [{"name": "staging", "path": str(_staging_root)}]
    if _s3_local_root and _s3_local_root.exists():
        roots.append({"name": "s3_local", "path": str(_s3_local_root)})
    return {"roots": roots}


@router.get("/list")
def list_dir(root: str = Query(...), path: str = Query("")):
    if root == "staging":
        base = _staging_root
    elif root == "s3_local" and _s3_local_root:
        base = _s3_local_root
    else:
        raise HTTPException(404, f"unknown root {root!r}")
    base.mkdir(parents=True, exist_ok=True)
    target = _safe_resolve(base, path)
    if not target.exists():
        return {"entries": [], "path": path}
    if not target.is_dir():
        raise HTTPException(400, "path is not a directory")

    entries = []
    for p in sorted(target.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower())):
        try:
            stat = p.stat()
            entries.append({
                "name": p.name,
                "is_dir": p.is_dir(),
                "size": stat.st_size if not p.is_dir() else 0,
                "mtime": stat.st_mtime,
                "suffix": p.suffix,
            })
        except Exception:
            continue
    return {"entries": entries, "path": path, "root": root}
