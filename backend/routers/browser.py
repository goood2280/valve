"""browser 라우터 — staging · s3_local · config · db 파일탐색기 (flow FileBrowser 경량 버전)."""
from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/api/browser", tags=["browser"])

_roots: dict[str, Path] = {}


def deps(staging_root: Path, s3_local_root: Path | None, extra_roots: dict[str, Path] | None = None):
    """extra_roots — 항상 노출할 추가 루트 (config: csv 설정파일, db: 파이프라인 산출물)."""
    global _roots
    _roots = {"staging": Path(staging_root)}
    if s3_local_root:
        _roots["s3_local"] = Path(s3_local_root)
    for name, p in (extra_roots or {}).items():
        _roots[name] = Path(p)


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
    return {"roots": [{"name": n, "path": str(p)} for n, p in _roots.items()
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

    # config 루트에 노출할 설정파일 확장자 (csv 는 표로, 나머지는 텍스트로 열람)
    CONFIG_SUFFIXES = {".csv", ".yaml", ".yml", ".json", ".txt", ".md"}
    entries = []
    for p in sorted(target.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower())):
        # config 루트는 설정파일(csv/yaml/json/…)만 노출 — 그 외 잡파일은 숨김
        if root == "config" and p.is_file() and p.suffix.lower() not in CONFIG_SUFFIXES:
            continue
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
