import sys
import yaml
import polars as pl
import pandas as pd
from pathlib import Path
import re

pl.disable_string_cache()

def load_config(name: str, path="config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    if name not in cfg:
        raise ValueError(f"{name} not found in config")
    return cfg[name]

vehicle_name = sys.argv[1]
config = load_config(vehicle_name, "config.yaml")
globals().update(config)

FEATURE_ROOT = Path(rf'D:\DB\3.FEATURE_STORE')
KEY_COLS = ["root_lot_id", "wafer_id"]

feat_dir = FEATURE_ROOT / vehicle
files = list(feat_dir.glob("*.parquet"))

if not files:
    raise ValueError("Feature 파일이 없습니다.")

print(f"{len(files)} feature files found")


def load_feature(path):
    return (
        pl.read_parquet(path)
        .with_columns([
            pl.col("root_lot_id").cast(pl.Utf8).str.strip_chars(),
            pl.col("wafer_id")
              .cast(pl.Utf8)
              .str.strip_chars()
              .str.replace_all(r"\D", "")
              .cast(pl.Int64)
        ])
        .unique(subset=KEY_COLS)  # 파일 내부 중복 제거
    )


# ---------------------------------
# 1️⃣ 모든 파일 로드
# ---------------------------------
dfs = [load_feature(f) for f in files]

# ---------------------------------
# 2️⃣ BASE wafer 집합 생성
# ---------------------------------
base_keys = (
    pl.concat([df.select(KEY_COLS) for df in dfs])
      .unique()
      .sort(KEY_COLS)
)

print(f"기준 wafer 수: {base_keys.height}")

# ---------------------------------
# 3️⃣ BASE 시작
# ---------------------------------
final_df = base_keys.clone()

# ---------------------------------
# 4️⃣ 각 feature LEFT JOIN
# ---------------------------------
for df in dfs:
    feature_cols = [c for c in df.columns if c not in KEY_COLS]

    final_df = final_df.join(
        df.select(KEY_COLS + feature_cols),
        on=KEY_COLS,
        how="left"
    )

# exception 빈열 추가
final_df = final_df.with_columns(
    pl.lit(None).alias("exception")
)
# vehicle 열 추가 = vehicle_name
# final_df = final_df.with_columns(
#     pl.lit(vehicle_name).alias("vehicle")
# )
final_df = final_df.with_columns(  
    pl.when(pl.lit(vehicle_name).str.contains("Ulysses0")).then(pl.lit("UlyssesEVT0"))  
    .when(pl.lit(vehicle_name).str.contains("Thetis1")).then(pl.lit("ThetisEVT1"))  
    .when(pl.lit(vehicle_name).str.contains("Ulysses1")).then(pl.lit("UlyssesEVT1")) 
    .otherwise(pl.lit(vehicle_name))  # 또는 기본값 설정 가능  
    .alias("PRODUCT")  
)  

# ---------------------------------
# 5️⃣ 저장 
# ---------------------------------
out_path = f"DB/ML_TABLE_{vehicle}.csv"

# VA 단 pass인 자재만 Filtering
# final_df = final_df.filter(
#     pl.col("FAB_CMP_tkout_status") == "PASSED"
# )
# print("VA 이후단만 Filtering")

# 각 컬럼의 null이 아닌 개수 계산
non_null_counts = final_df.select(pl.all().count()).row(0)

# 값이 하나라도 있는 컬럼만 남기기
# keep_cols = [col for col, cnt in zip(final_df.columns, non_null_counts) if cnt > 0]
keep_cols = [
    col for col, cnt in zip(final_df.columns, non_null_counts)
    if (cnt > 0) or (col == "exception")
]

final_df = final_df.select(keep_cols)

# 1️⃣ Polars → Pandas 변환
final_df = final_df.rename({
    "root_lot_id": "ROOT_LOT_ID",
    "wafer_id": "WAFER_ID"
})
pdf = final_df.to_pandas()

# 열 정렬

def first_number_after(prefix, col):
    # prefix 뒤 문자열만 자름
    part = col.split(prefix, 1)[-1]

    # 첫 번째 숫자(정수/실수) 찾기
    m = re.search(r'\d+(?:\.\d+)?', part)
    return float(m.group()) if m else float('inf')

cols = pdf.columns.tolist()

# 3️⃣ 고정 컬럼
product_col = [c for c in cols if c == "PRODUCT"]
root_col    = [c for c in cols if c == "ROOT_LOT_ID"]
wafer_col   = [c for c in cols if c == "WAFER_ID"]

# 4️⃣ 패턴 컬럼 (첫 숫자 기준 정렬)
split_cols  = sorted([c for c in cols if c.startswith("KNOB_")],  key=lambda x: first_number_after("KNOB_", x))
fab_cols    = sorted([c for c in cols if c.startswith("FAB_")],    key=lambda x: first_number_after("FAB_", x))
inline_cols = sorted([c for c in cols if c.startswith("INLINE_")], key=lambda x: first_number_after("INLINE_", x))
vm_cols     = sorted([c for c in cols if c.startswith("VM_")],     key=lambda x: first_number_after("VM_", x))

# 5️⃣ 우선순위 순서
ordered_cols = (
    product_col
    + root_col
    + wafer_col
    + split_cols
    + fab_cols
    + inline_cols
    + vm_cols
)

# 6️⃣ 나머지 컬럼 자동 뒤에 붙이기 ⭐ (핵심)
remaining_cols = [c for c in cols if c not in ordered_cols]

pdf = pdf[ordered_cols + remaining_cols]

#KNOB열 None 제거
pdf.loc[:, pdf.columns.str.startswith("KNOB_")] = (
    pdf.loc[:, pdf.columns.str.startswith("KNOB_")]
        .replace("None", "")
        .replace("None_None", "")
        .replace("None_None_None", "")
        .where(lambda x: x.notna(), "")
)

# 2️⃣ UTF-8 BOM 포함 저장 (엑셀 한글 안 깨짐)
# pdf.head(100).to_csv(f"Sample wide {vehicle}.csv", index=False, encoding="utf-8-sig")
# pdf.to_csv(out_path, index=False, encoding="utf-8-sig")

# 1) 샘플 100행 → CSV 그대로 유지 (엑셀용)
pdf.head(100).to_csv(
    f"Sample_wide_{vehicle}.csv",
    index=False,
    encoding="utf-8-sig"
)

# 2) 전체 데이터 → Parquet 저장
pl_df = pl.from_pandas(pdf)

parquet_path = out_path.replace(".csv", ".parquet")

pl_df.write_parquet(
    parquet_path,
    compression="zstd",     # 용량↓ 속도↑
    statistics=True         # ★ LOT 조회 속도 핵심 옵션
)

# final_df.write_csv(out_path, encoding="utf8-lossy", include_bom=True)
print(f"ML 테이블 생성 완료 → {out_path}")
