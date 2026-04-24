"""query 라우터 — parquet head + polars SQL 필터."""
from __future__ import annotations

from pathlib import Path

import polars as pl
from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/api/query", tags=["query"])

_staging_root: Path = None
_s3_local_root: Path = None

MAX_ROWS = 2000


def deps(staging_root: Path, s3_local_root: Path | None):
    global _staging_root, _s3_local_root
    _staging_root = Path(staging_root)
    _s3_local_root = Path(s3_local_root) if s3_local_root else None


def _resolve(root: str, rel: str) -> Path:
    if root == "staging":
        base = _staging_root
    elif root == "s3_local" and _s3_local_root:
        base = _s3_local_root
    else:
        raise HTTPException(404, f"unknown root {root!r}")
    target = (base / rel).resolve()
    try:
        target.relative_to(base.resolve())
    except ValueError:
        raise HTTPException(400, "path escape")
    return target


def _apply_sql(lf: pl.LazyFrame, sql: str) -> pl.LazyFrame:
    sql = (sql or "").strip()
    if not sql:
        return lf
    ctx = pl.SQLContext(frames={"t": lf}, eager=False)
    # 'from t' 가 포함돼야 함 — 없으면 WHERE/SELECT 파편으로 보고 감쌈
    low = sql.lower()
    if "from" not in low:
        sql = f"SELECT * FROM t WHERE {sql}"
    elif "from t" not in low and "from T" not in sql:
        # 사용자가 다른 table alias 쓰면 그대로
        pass
    return ctx.execute(sql)


@router.get("/view")
def view(root: str = Query(...), file: str = Query(...), sql: str = Query(""),
         rows: int = Query(200, ge=1, le=MAX_ROWS)):
    p = _resolve(root, file)
    if not p.exists():
        raise HTTPException(404, "file not found")
    if p.suffix.lower() != ".parquet":
        raise HTTPException(400, "only .parquet supported in v0.1")

    try:
        lf = pl.scan_parquet(str(p))
        if sql:
            try:
                lf = _apply_sql(lf, sql)
            except Exception as e:
                raise HTTPException(400, f"sql_error: {e}")
        df = lf.limit(rows).collect()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"read_error: {e}")

    return {
        "columns": df.columns,
        "rows": df.to_dicts(),
        "n_rows": df.height,
        "dtypes": {c: str(t) for c, t in zip(df.columns, df.dtypes)},
    }
