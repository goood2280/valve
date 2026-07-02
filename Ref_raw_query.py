import sys
import yaml
import pandas as pd
import polars as pl
from bigdataquery import *
from datetime import datetime, timedelta
from pathlib import Path

def load_config(name: str, path="config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    if name not in cfg:
        raise ValueError(f"{name} not found in config")

    return cfg[name]
    
def get_split_date_ranges(query_span_days: int, split_span_days: int):

    today = datetime.today().date()
    start_base = today - timedelta(days=query_span_days)

    ranges = []
    current_start = start_base

    while current_start < today:
        current_end = current_start + timedelta(days=split_span_days)

        if current_end > today:
            current_end = today

        ranges.append((current_start, current_end))
        current_start += timedelta(days=split_span_days)

    ranges.append((today, today))

    return ranges

######################################################################
# Config
TABLE = 'TABLE_NAME'
LINE_ID = ['LINE']
USER_NAME = 'USER_NAME'
DB_DIR = r'D:\DB\1.RAWDATA_DB' #'DB/1.RAWDATA_DB'
######################################################################

vehicle_name = sys.argv[1]  
config = load_config(vehicle_name, f"config.yaml")

globals().update(config)

date_ranges = get_split_date_ranges(QueryTimeSpan, SplitTimeSpan)
print(date_ranges)
for start_date, end_date in date_ranges:
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str   = end_date.strftime("%Y-%m-%d")
    dfs = []
    result_df = pd.DataFrame()
    print(f'{start_str}~{end_str} FAB Data Query 수행')
    params = {
            'table_name': TABLE,
            'dateFrom': start_str,
            'dateTo': end_str,
            'process_id' : process_id,
            'line_id': LINE_ID
            }
    Query_Table = getData(params, user_name=USER_NAME)

    DB_DIR = Path(DB_DIR)
    save_dir = DB_DIR / vehicle / f"date={start_date.strftime('%Y-%m-%d')}"
    save_dir.mkdir(parents=True, exist_ok=True)

    df_pl = pl.from_pandas(Query_Table)
    df_pl.write_parquet(
        f"{DB_DIR}/{vehicle}/date={start_date}/part-000.parquet",
        compression="zstd",
        compression_level=3  
    )
