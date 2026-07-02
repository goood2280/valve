from pathlib import Path
import pandas as pd
import sys
import yaml
from datetime import datetime, timedelta
import polars as pl

def load_config(name: str, path="config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    if name not in cfg:
        raise ValueError(f"{name} not found in config")

    return cfg[name]

vehicle_name = sys.argv[1]  
config = load_config(vehicle_name, f"config.yaml")
globals().update(config)

RAW_DIR = Path(rf'D:\DB\1.RAWDATA_DB\{vehicle}') #DB/1.RAWDATA_DB/{vehicle}
EVENT_DIR = Path(rf'D:\DB\2.EVENT_DB\{vehicle}') #DB/2.EVENT_DB/{vehicle}
STEP_MAP_PATH = Path("STEP_MATCHING/Vehicle_matching.csv")

# step 매핑 테이블
step_map = pl.read_csv(STEP_MAP_PATH)

# EVENT에 남길 컬럼
KEEP_COLS = [
    "root_lot_id",
    "wafer_id",
    "part_id",
    "tkout_time",
    "step_id",
    "step_desc",
    "ppid",
    "reticle_id",
    "eqp_id",
    "chamber_id",
    "unit_id",
    "sleuth_order"
]

# 🔹 날짜 리스트
today = datetime.today().date()
DATE_FOLDERS = [(today - timedelta(days=i)).strftime("date=%Y-%m-%d")
                for i in range(event_days_back)]

# 🔹 처리 루프
for date_folder in DATE_FOLDERS:

    raw_path = RAW_DIR / date_folder / "part-000.parquet"
    if not raw_path.exists():
        continue

    print(f"▶ Processing {raw_path}")

    raw = pl.read_parquet(raw_path)

    # step_id 형 변환 매칭
    raw = raw.with_columns(
        pl.col("step_id").cast(pl.Utf8)
    )
    
    step_map = step_map.with_columns(
        pl.col("step_id").cast(pl.Utf8)
    )

    event = (
        raw
        .join(step_map, on="step_id", how="inner")
        .filter(
            pl.col("root_lot_id")
            .cast(pl.Utf8)
            .str.starts_with(event_lot_startwith)
        )
        .select(KEEP_COLS)
        .select(pl.all().cast(pl.String))
        )

    # 저장
    save_dir = EVENT_DIR / date_folder
    save_dir.mkdir(parents=True, exist_ok=True)

    event.write_parquet(
        save_dir / "part-000.parquet")
