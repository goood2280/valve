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

# raw vm
import sys
import yaml
import pandas as pd
import polars as pl
from bigdataquery import *
from datetime import datetime, timedelta
from pathlib import Path
import math

def load_config(name: str, path="config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    if name not in cfg:
        raise ValueError(f"{name} not found in config")

    return cfg[name]
    
def get_split_date_ranges(query_span_days: int, split_span_days: int):
    """
    query_span_days: 전체 기간 (예: 3일)
    split_span_days: 각 쿼리 구간 길이 (예: 1 → 하루폭)

    return: [(start_date, end_date), ...]
    """

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
TABLE = 'table_VM'
LINE_ID = ['line']
USER_NAME = 'name'
DB_DIR = r'D:\DB\1.RAWDATA_DB_VM' #'DB/1.RAWDATA_DB_VM'
######################################################################

vehicle_name = sys.argv[1]  
config = load_config(vehicle_name, f"config.yaml")

globals().update(config)

date_ranges = get_split_date_ranges(QueryTimeSpan, SplitTimeSpan)

for start_date, end_date in date_ranges:
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str   = end_date.strftime("%Y-%m-%d")
    dfs = []
    result_df = pd.DataFrame()
    print(f'{start_str}~{end_str} VM Query 수행')
    # ROOT LOT 단위로 분리 수행
    col_check = ['root_lot_id']
    chunk_num = QueryTimeSpan_vm_chunk

    params = {
            'table_name': TABLE,
            'dateFrom': start_str,
            'dateTo': end_str,
            'process_id' : process_id,
            'line_id': LINE_ID
            }
    temp_query = getData(params, custom_columns = col_check, user_name=USER_NAME)
    root_lots = temp_query['root_lot_id'].dropna().unique().tolist()

    def split_list(lst, n):
        k = math.ceil(len(lst) / n)   # chunk size
        return [lst[i:i+k] for i in range(0, len(lst), k)]
    
    chunks = split_list(root_lots, chunk_num)
    
    for i, lot_chunk in enumerate(chunks):
        
        print(f'{start_date} 작성시작, chunk : {chunk_num}')
        # print(f'lot_list = {lot_chunk}')
        params = {
                'table_name': TABLE,
                'dateFrom': start_str,
                'dateTo': end_str,
                'process_id' : process_id,
                'root_lot_id' : lot_chunk,
                'line_id': LINE_ID
                }
        Query_Table = getData(params, user_name=USER_NAME)

        DB_DIR = Path(DB_DIR)
        save_dir = DB_DIR / vehicle / f"date={start_date.strftime('%Y-%m-%d')}"
        save_dir.mkdir(parents=True, exist_ok=True)

        df_pl = pl.from_pandas(Query_Table)
        df_pl.write_parquet(
            f"{DB_DIR}/{vehicle}/date={start_date}/part-00{i}.parquet",
            compression="zstd",
            compression_level=3  
        )

#raw inline
import sys
import yaml
import pandas as pd
import polars as pl
from bigdataquery import *
from datetime import datetime, timedelta
from pathlib import Path
import math

def load_config(name: str, path="config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    if name not in cfg:
        raise ValueError(f"{name} not found in config")

    return cfg[name]
    
def get_split_date_ranges(query_span_days: int, split_span_days: int):
    """
    query_span_days: 전체 기간 (예: 3일)
    split_span_days: 각 쿼리 구간 길이 (예: 1 → 하루폭)

    return: [(start_date, end_date), ...]
    """

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
TABLE = 'table_inline'
LINE_ID = ['line']
USER_NAME = 'name'
DB_DIR = r'D:\DB\1.RAWDATA_DB_INLINE' #'DB/1.RAWDATA_DB_INLINE'
######################################################################

vehicle_name = sys.argv[1]  
config = load_config(vehicle_name, f"config.yaml")

globals().update(config)

date_ranges = get_split_date_ranges(QueryTimeSpan, SplitTimeSpan)

for start_date, end_date in date_ranges:
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str   = end_date.strftime("%Y-%m-%d")
    dfs = []
    result_df = pd.DataFrame()
    print(f'{start_str}~{end_str} Inline Query 수행')
    # ROOT LOT 단위로 분리 수행
    col_check = ['root_lot_id']
    chunk_num = QueryTimeSpan_Inline_chunk

    params = {
            'table_name': TABLE,
            'dateFrom': start_str,
            'dateTo': end_str,
            'process_id' : process_id,
            'line_id': LINE_ID
            }
    temp_query = getData(params, custom_columns = col_check, user_name=USER_NAME)
    root_lots = temp_query['root_lot_id'].dropna().unique().tolist()

    def split_list(lst, n):
        k = math.ceil(len(lst) / n)   # chunk size
        return [lst[i:i+k] for i in range(0, len(lst), k)]
    
    chunks = split_list(root_lots, chunk_num) 

    for i, lot_chunk in enumerate(chunks):
        print(f'{start_date} 작성시작, chunk : {chunk_num}')
        # print(f'lot_list = {lot_chunk}')
        params = {
                'table_name': TABLE,
                'dateFrom': start_str,
                'dateTo': end_str,
                'process_id' : process_id,
                'root_lot_id' : lot_chunk,
                'line_id': LINE_ID
                }
        Query_Table = getData(params, user_name=USER_NAME)

        DB_DIR = Path(DB_DIR)
        save_dir = DB_DIR / vehicle / f"date={start_date.strftime('%Y-%m-%d')}"
        save_dir.mkdir(parents=True, exist_ok=True)

        df_pl = pl.from_pandas(Query_Table)
        df_pl.write_parquet(
            f"{DB_DIR}/{vehicle}/date={start_date}/part-00{i}.parquet",
            compression="zstd",
            compression_level=3  
        )
