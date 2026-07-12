"""query 라우터 — parquet/csv head + polars SQL 필터. 루트 해석은 browser 라우터와 공유."""
from __future__ import annotations

import polars as pl
from fastapi import APIRouter, HTTPException, Query

from backend.routers.browser import resolve

router = APIRouter(prefix="/api/query", tags=["query"])

MAX_ROWS = 2000
# 표(parquet/csv)가 아닌 설정파일은 텍스트로 미리보기
TEXT_SUFFIXES = {".yaml", ".yml", ".json", ".txt", ".md", ".py"}
MAX_TEXT_BYTES = 256 * 1024


def _apply_sql(lf: pl.LazyFrame, sql: str) -> pl.LazyFrame:
    sql = (sql or "").strip()
    if not sql:
        return lf
    ctx = pl.SQLContext(frames={"t": lf}, eager=False)
    # 'from t' 가 포함돼야 함 — 없으면 WHERE/SELECT 파편으로 보고 감쌈
    if "from" not in sql.lower():
        sql = f"SELECT * FROM t WHERE {sql}"
    return ctx.execute(sql)


@router.get("/view")
def view(root: str = Query(...), file: str = Query(...), sql: str = Query(""),
         rows: int = Query(200, ge=1, le=MAX_ROWS)):
    p = resolve(root, file)
    if not p.exists():
        raise HTTPException(404, "file not found")
    suffix = p.suffix.lower()

    # 설정파일(yaml/json/txt/md)은 텍스트 그대로 반환
    if suffix in TEXT_SUFFIXES:
        raw = p.read_bytes()[:MAX_TEXT_BYTES]
        return {"kind": "text", "suffix": suffix,
                "text": raw.decode("utf-8", errors="replace"),
                "truncated": p.stat().st_size > MAX_TEXT_BYTES}

    if suffix not in (".parquet", ".csv"):
        raise HTTPException(400, "지원 형식: parquet · csv · yaml · json · txt · md")

    try:
        lf = pl.scan_parquet(str(p)) if suffix == ".parquet" else pl.scan_csv(str(p))
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
        "kind": "table",
        "columns": df.columns,
        "rows": df.to_dicts(),
        "n_rows": df.height,
        "dtypes": {c: str(t) for c, t in zip(df.columns, df.dtypes)},
    }
