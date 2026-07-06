import polars as pl
from pathlib import Path
import sys
import yaml
import re

def load_config(name: str, path="config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    if name not in cfg:
        raise ValueError(f"{name} not found in config")

    return cfg[name]

vehicle_name = sys.argv[1]  
config = load_config(vehicle_name, f"config.yaml")
globals().update(config)

# ======================
# 기본 설정
# ======================

EVENT_PATH = rf'D:\DB\2.EVENT_DB\{vehicle}\**\*.parquet' #f"DB/2.EVENT_DB/{vehicle}/**/*.parquet"
FEATURE_STORE = Path(rf'D:\DB\3.FEATURE_STORE\{vehicle}')
FEATURE_STORE.mkdir(exist_ok=True)
FAB_PATH = f"FAB/fab.csv"

KEY_COLS = ["root_lot_id", "wafer_id"]

# ======================
# 값 생성 함수
# ======================
def safe_filename(name: str) -> str:
    # 경로 문자 및 위험문자 제거
    name = re.sub(r'[\\/:\*\?"<>\|]+', '_', name)
    # 공백 정리
    name = re.sub(r'\s+', '_', name)
    # 끝 점 제거 (윈도우 문제 방지)
    name = name.rstrip('.')
    return name

def clean_str(col):
    return (
        pl.col(col)
        .cast(pl.Utf8)
        .str.strip_chars()
        .replace("", None)   # 🔥 빈 문자열 → null
    )

def build_eqp_all():

    eqp = clean_str("eqp_id")
    chamber = clean_str("chamber_id")
    unit = clean_str("unit_id")
    tk = clean_str("tkout_time")

    tool_part = pl.concat_str(
        [eqp, chamber, unit],
        separator="_",
        ignore_nulls=True
    )

    return pl.concat_str(
        [tk, tool_part],
        separator="|",
        ignore_nulls=True
    )

def build_ecu_all():

    def clean_dash(col):
        c = clean_str(col).str.strip_chars()

        return (
            pl.when(
                (c == "-") | (c == "") | (c.is_null())
            )
            .then(None)
            .otherwise(c)
        )

    eqp = clean_dash("eqp_id")
    chamber = clean_dash("chamber_id")
    unit = clean_dash("unit_id")

    tool_part = pl.concat_str(
        [eqp, chamber, unit],
        separator="_",
        ignore_nulls=True
    )

    return tool_part

def build_part_reticle():

    part = clean_str("part_id").str.slice(0, 10)
    reticle = clean_str("reticle_id")

    tool_part = pl.concat_str(
        [part, reticle],
        separator="|",
        ignore_nulls=True
    )

    return tool_part

def is_null_like(expr):
    return expr.is_null() | (expr.str.strip_chars() == "")

def apply_check_df(df, col, check):

    if check is None or str(check).strip() == "":
        return df

    check = str(check).strip().lower()
    val = pl.col(col).cast(pl.Utf8).str.strip_chars()

    if check.startswith("eq "):
        target = check[3:].strip().upper()
        return df.filter(val.str.to_uppercase() == target)

    elif check == "not null":
        return df.filter(~is_null_like(val))

    elif check == "null":
        return df.filter(is_null_like(val))

    elif check.startswith("startswith"):
        target = check.split("startswith")[1].strip().upper()
        return df.filter(val.str.to_uppercase().str.starts_with(target))

    elif check.startswith("contains"):
        target = check.split("contains")[1].strip().upper()
        return df.filter(val.str.to_uppercase().str.contains(target))

    return df

FEATURE_RULES = {
    "eqp_id": lambda: pl.col("eqp_id").cast(pl.Utf8),
    "chamber_id": lambda: pl.col("chamber_id").cast(pl.Utf8),
    "unit_id": lambda: pl.col("unit_id").cast(pl.Utf8),
    "part_id": lambda: pl.col("part_id").cast(pl.Utf8).str.slice(0, 10),
    "reticle_id": lambda: pl.col("reticle_id").cast(pl.Utf8),
    "ppid": lambda: pl.col("ppid").cast(pl.Utf8),
    "tkout_time": lambda: pl.col("tkout_time").cast(pl.Utf8),
    "tkout_status": lambda: (
                        pl.when(pl.col("tkout_time").is_not_null())
                          .then(pl.lit("PASSED"))
                          .otherwise(pl.lit("NOT_PASSED"))
                    ),
    "sleuth_order": lambda: pl.col("sleuth_order").cast(pl.Utf8),
    "eqpall": build_eqp_all,
    "ecuall": build_ecu_all,
    "reticleall": build_part_reticle
}

# ======================
# 집계 함수
# ======================
def aggregate_feature(df, feature_col, agg_type):
    if agg_type == "first":
        return (
            df.sort("tkout_time")
              .group_by(KEY_COLS)
              .agg(pl.col("val").first().alias(feature_col))
        )
    elif agg_type == "last":
        return (
            df.sort("tkout_time")
              .group_by(KEY_COLS)
              .agg(pl.col("val").last().alias(feature_col))
        )
        
    elif agg_type == "concat":
        return (
            df.sort("tkout_time")
              .group_by(KEY_COLS)
              .agg(
                  pl.col("val")
                    .cast(pl.Utf8)
                    .str.strip_chars()
                    .str.join("_")
                    .alias(feature_col)
              )
        )
    
    elif agg_type == "last_valid":

        c = (
            pl.col("val")
            .cast(pl.Utf8)
            .str.strip_chars()
            .str.to_uppercase()
        )

        return (
            df.sort("tkout_time")
            .with_columns(
                pl.when(
                    c.is_null() |
                    (c == "") |
                    (c == "-") |
                    c.str.contains("SKIP")   # ← 핵심: 포함된 SKIP 전부 제거
                )
                .then(None)
                .otherwise(c)
                .alias("val_clean")
            )
            .group_by(KEY_COLS)
            .agg(
                pl.col("val_clean")
                    .drop_nulls()
                    .last()
                    .alias(feature_col)
            )
        )

    elif agg_type == "valid_eqp":
        return (
            df.with_columns(
                pl.col("val")
                  .cast(pl.Utf8)
                  .str.strip_chars()
                  .alias("val_str")
            )
            .filter(
                pl.col("val_str")
                  .str.contains(r"_[A-Za-z0-9]*[0-9]") #.str.extract(r"_([0-9]+)$", 1)   # _뒤 숫자 추출
                #.is_not_null()
            )
            .sort("tkout_time")
            .group_by(KEY_COLS)
            .agg(
                pl.col("val_str")
                  .first()
                  .alias(feature_col)
            )
        )
        
    elif agg_type == "agg":
        return (
            df.group_by(KEY_COLS)
              .agg(pl.col("val").unique().sort().str.join("_").alias(feature_col))
        )

# ======================
# EVENT 로드
# ======================

event = pl.scan_parquet(EVENT_PATH)
# event = pl.scan_csv(
#     EVENT_PATH,
#     infer_schema_length=0
# ).select(
#     pl.all().cast(pl.String)
# )

rules = pl.read_csv(FAB_PATH)

total = rules.height  # 또는 len(rules)

for idx, r in enumerate(rules.iter_rows(named=True), start=1):

    step = r["step_desc"]
    fname = r["feature_name"].replace("\n", "").replace("\r", "")
    agg_type = r["agg"]
    feature_col = f"{step}_{fname}"

    percent = (idx / total) * 100
    remaining = total - idx

    print(
        f"[{idx}/{total}] ({percent:6.2f}%) | Processing feature: {feature_col}"
    )

# ======================
# Single Step Feature 생성
# ======================

    # step 필터
    df = event.filter(pl.col("step_desc") == step)

    # feature 값 생성 (feature_name 기준)
    val_expr = FEATURE_RULES[fname]().alias("val")
    df = df.with_columns(val_expr)

    # 집계
    feat = aggregate_feature(df, feature_col, agg_type)

    rename_map = {
        c: f"FAB_{c}"
        for c in feat.columns
        if c not in KEY_COLS
    }

    feat = feat.rename(rename_map)

    safe_feature_col = safe_filename(feature_col)
    file_name = f"FAB_{safe_feature_col}.parquet"

    # 실제 저장 대상 컬럼
    fab_cols = list(rename_map.values())

    # 저장할 FAB 컬럼이 아예 없으면 skip
    if fab_cols:
        feat_df = feat.collect()

        check_exprs = []
        for c in fab_cols:
            dtype = feat_df.schema[c]

            if dtype == pl.String:
                # null / 빈문자열 / 공백만 있는 문자열 제외
                check_exprs.append(
                    pl.col(c).is_not_null() & (pl.col(c).str.strip_chars() != "")
                )
            elif dtype.is_float():
                # null / NaN 제외
                check_exprs.append(
                    pl.col(c).is_not_null() & (~pl.col(c).is_nan())
                )
            else:
                # 그 외 타입은 null 아니면 값 있다고 판단
                check_exprs.append(
                    pl.col(c).is_not_null()
                )

        has_any_value = feat_df.select(
            pl.any_horizontal(check_exprs).any()
        ).item()

        if has_any_value:
            feat_df.write_parquet(FEATURE_STORE / file_name)

#Feature VM
import polars as pl
from pathlib import Path
import sys
import yaml
import re

def safe_name(s: str) -> str:
    if s is None:
        return "UNKNOWN"

    # 줄바꿈 제거
    s = s.replace("\n", "").replace("\r", "")

    # 공백 정리
    s = s.strip()

    # 파일시스템 위험 문자 치환
    s = re.sub(r'[\\/:*?"<>|]', '_', s)

    # 연속 _ 정리
    s = re.sub(r'_+', '_', s)

    return s
    
def load_config(name: str, path="config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    if name not in cfg:
        raise ValueError(f"{name} not found in config")

    return cfg[name]

vehicle_name = sys.argv[1]  
config = load_config(vehicle_name, f"config.yaml")
globals().update(config)

# ======================
# 기본 설정
# ======================

EVENT_PATH = rf'D:\DB\2.EVENT_DB_VM\{vehicle}\**\*.parquet' #f"DB/2.EVENT_DB/{vehicle}/**/*.parquet"
FEATURE_STORE = Path(rf'D:\DB\3.FEATURE_STORE\{vehicle}')

# EVENT_PATH = f"DB/2.EVENT_DB_VM/{vehicle}/**/*.parquet"
# FEATURE_STORE = Path(f"DB/3.FEATURE_STORE/{vehicle}")
FEATURE_STORE.mkdir(exist_ok=True)
VM_PATH = f"VM/vm_matching.csv"

KEY_COLS = ["root_lot_id", "wafer_id"]

# ======================
# EVENT 로드 (Lazy 유지)
# ======================

event = (
    pl.scan_parquet(EVENT_PATH)
    .select([
        "root_lot_id",
        "wafer_id",
        "step_desc",
        "subitem_id",
        "item_id",
        "tkout_time",
        "fab_value"
    ])
    .with_columns([
        pl.col("step_desc").cast(pl.Utf8),
        pl.col("item_id").cast(pl.Utf8),
    ])
)

rules = (
    pl.scan_csv(VM_PATH)
    .select(["step_desc", "item_id"])
    .with_columns([
        pl.col("step_desc").cast(pl.Utf8),
        pl.col("item_id").cast(pl.Utf8),
    ])
)

# 매칭
matched = event.join(
    rules,
    on=["step_desc", "item_id"],
    how="inner"
)

# ★ 2️⃣ subitem_id 가 "VALUE" 인 행만 남기기  
matched = matched.filter(pl.col("subitem_id") == "VALUE")

last_value = (
    matched
    .group_by(["root_lot_id", "wafer_id", "step_desc", "item_id"])
    .agg([
        pl.col("fab_value")
          .sort_by("tkout_time")
          .last()
          .alias("feature_value")
    ])
    .filter(pl.col("feature_value").cast(pl.Float64) != 0)
    .with_columns(
        pl.concat_str(
            [
                pl.lit("VM_"),
                pl.col("step_desc"),
                pl.lit("_"),
                pl.col("item_id"),
            ]
        ).alias("feature")
    )
    .select([
        "root_lot_id",
        "wafer_id",
        "feature",
        "feature_value"
    ])
)

# ======================
# 2️⃣ Lazy → 한 번만 실행
# ======================

df = last_value.collect(streaming=True)

# ======================
# 3️⃣ 저장 옵션
# ======================

SAVE_PARQUET = True
SAVE_CSV = False  

feature_groups = df.partition_by("feature", as_dict=True)
total = len(feature_groups)

# 아웃라이어 nan화 함수
def remove_outliers_mad_polars(df: pl.DataFrame, col: str, n: float = 100, min_mad: float = 1e-9):

    med = df.select(pl.col(col).median()).item()
    mad = df.select((pl.col(col) - med).abs().median()).item()

    if mad is None or mad < min_mad:
        return df

    lower = med - n * mad
    upper = med + n * mad

    return df.with_columns(
        pl.when((pl.col(col) < lower) | (pl.col(col) > upper))
        .then(None)
        .otherwise(pl.col(col))
        .alias(col)
    )

for idx, (feature_key, subdf) in enumerate(feature_groups.items(), start=1):

    # tuple → 실제 feature 값
    feature = feature_key[0] if isinstance(feature_key, tuple) else feature_key

    percent = (idx / total) * 100
    print(f"[{idx}/{total}] ({percent:6.2f}%) Saving: {feature}")

    output_df = (
        subdf
        .select([
            "root_lot_id",
            "wafer_id",
            pl.col("feature_value")
            .cast(pl.Float64) 
            .alias(feature)   # 🔥 컬럼명 변경
        ])
    )

    output_df = remove_outliers_mad_polars(
        output_df,
        col=feature,
        n=100
    )

    base_path = f"{FEATURE_STORE}/{feature}"

    if SAVE_PARQUET:
        output_df.write_parquet(base_path + ".parquet")

    if SAVE_CSV:
        output_df.write_csv(base_path + ".csv")

# feature Inline
import polars as pl
from pathlib import Path
import sys
import yaml
import re

def safe_name(s: str) -> str:
    if s is None:
        return "UNKNOWN"

    # 줄바꿈 제거
    s = s.replace("\n", "").replace("\r", "")

    # 공백 정리
    s = s.strip()

    # 파일시스템 위험 문자 치환
    s = re.sub(r'[\\/:*?"<>|]', '_', s)

    # 연속 _ 정리
    s = re.sub(r'_+', '_', s)

    return s
    
def load_config(name: str, path="config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    if name not in cfg:
        raise ValueError(f"{name} not found in config")

    return cfg[name]

vehicle_name = sys.argv[1]  
config = load_config(vehicle_name, f"config.yaml")
globals().update(config)

# ======================
# 기본 설정
# ======================

EVENT_PATH = rf'D:\DB\2.EVENT_DB_VM\{vehicle}\**\*.parquet' #f"DB/2.EVENT_DB/{vehicle}/**/*.parquet"
FEATURE_STORE = Path(rf'D:\DB\3.FEATURE_STORE\{vehicle}')

# EVENT_PATH = f"DB/2.EVENT_DB_VM/{vehicle}/**/*.parquet"
# FEATURE_STORE = Path(f"DB/3.FEATURE_STORE/{vehicle}")
FEATURE_STORE.mkdir(exist_ok=True)
VM_PATH = f"VM/vm_matching.csv"

KEY_COLS = ["root_lot_id", "wafer_id"]

# ======================
# EVENT 로드 (Lazy 유지)
# ======================

event = (
    pl.scan_parquet(EVENT_PATH)
    .select([
        "root_lot_id",
        "wafer_id",
        "step_desc",
        "subitem_id",
        "item_id",
        "tkout_time",
        "fab_value"
    ])
    .with_columns([
        pl.col("step_desc").cast(pl.Utf8),
        pl.col("item_id").cast(pl.Utf8),
    ])
)

rules = (
    pl.scan_csv(VM_PATH)
    .select(["step_desc", "item_id"])
    .with_columns([
        pl.col("step_desc").cast(pl.Utf8),
        pl.col("item_id").cast(pl.Utf8),
    ])
)

# 매칭
matched = event.join(
    rules,
    on=["step_desc", "item_id"],
    how="inner"
)

# ★ 2️⃣ subitem_id 가 "VALUE" 인 행만 남기기  
matched = matched.filter(pl.col("subitem_id") == "VALUE")

last_value = (
    matched
    .group_by(["root_lot_id", "wafer_id", "step_desc", "item_id"])
    .agg([
        pl.col("fab_value")
          .sort_by("tkout_time")
          .last()
          .alias("feature_value")
    ])
    .filter(pl.col("feature_value").cast(pl.Float64) != 0)
    .with_columns(
        pl.concat_str(
            [
                pl.lit("VM_"),
                pl.col("step_desc"),
                pl.lit("_"),
                pl.col("item_id"),
            ]
        ).alias("feature")
    )
    .select([
        "root_lot_id",
        "wafer_id",
        "feature",
        "feature_value"
    ])
)

# ======================
# 2️⃣ Lazy → 한 번만 실행
# ======================

df = last_value.collect(streaming=True)

# ======================
# 3️⃣ 저장 옵션
# ======================

SAVE_PARQUET = True
SAVE_CSV = False  

feature_groups = df.partition_by("feature", as_dict=True)
total = len(feature_groups)

# 아웃라이어 nan화 함수
def remove_outliers_mad_polars(df: pl.DataFrame, col: str, n: float = 100, min_mad: float = 1e-9):

    med = df.select(pl.col(col).median()).item()
    mad = df.select((pl.col(col) - med).abs().median()).item()

    if mad is None or mad < min_mad:
        return df

    lower = med - n * mad
    upper = med + n * mad

    return df.with_columns(
        pl.when((pl.col(col) < lower) | (pl.col(col) > upper))
        .then(None)
        .otherwise(pl.col(col))
        .alias(col)
    )

for idx, (feature_key, subdf) in enumerate(feature_groups.items(), start=1):

    # tuple → 실제 feature 값
    feature = feature_key[0] if isinstance(feature_key, tuple) else feature_key

    percent = (idx / total) * 100
    print(f"[{idx}/{total}] ({percent:6.2f}%) Saving: {feature}")

    output_df = (
        subdf
        .select([
            "root_lot_id",
            "wafer_id",
            pl.col("feature_value")
            .cast(pl.Float64) 
            .alias(feature)   # 🔥 컬럼명 변경
        ])
    )

    output_df = remove_outliers_mad_polars(
        output_df,
        col=feature,
        n=100
    )

    base_path = f"{FEATURE_STORE}/{feature}"

    if SAVE_PARQUET:
        output_df.write_parquet(base_path + ".parquet")

    if SAVE_CSV:
        output_df.write_csv(base_path + ".csv")

#feature qtime
import polars as pl
from pathlib import Path
import sys
import yaml
import re

def load_config(name: str, path="config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    if name not in cfg:
        raise ValueError(f"{name} not found in config")

    return cfg[name]

vehicle_name = sys.argv[1]  
config = load_config(vehicle_name, f"config.yaml")
globals().update(config)

# ======================
# 기본 설정
# ======================

EVENT_PATH = rf'D:\DB\2.EVENT_DB_QTIME\{vehicle}\**\*.parquet' #f"DB/2.EVENT_DB/{vehicle}/**/*.parquet"
FEATURE_STORE = Path(rf'D:\DB\3.FEATURE_STORE\{vehicle}')

# EVENT_PATH = f"DB/2.EVENT_DB_QTIME/{vehicle}/**/*.parquet"
# FEATURE_STORE = Path(f"DB/3.FEATURE_STORE/{vehicle}")
FEATURE_STORE.mkdir(exist_ok=True)

KEY_COLS = ["root_lot_id", "wafer_id"]
STEP_COL = "step_desc"
PPID_COL = "ppid"
A_TIME_COL = "chamber_end_time" # A_TIME - B_TIME = QTIME으로 산출됨
B_TIME_COL = "wafer_end_time"
C_TIME_COL = "tkin_time" #해당시간이 최신인 WF 만 살림

SAVE_CSV = False

#옵션 해당 Step_desc에서 ppid BBB인 WF는 제하고 tkin_time이 최신인 데이터중에 계산
EXCLUDE_STEP_DESC = "AAA"
EXCLUDE_PPID = "BBBB"

# ======================
# EVENT 로드
# ======================
def safe_name(x: str) -> str:
    x = str(x).strip()
    x = re.sub(r"[^\w\-.]+", "_", x)
    x = re.sub(r"_+", "_", x)
    return x.strip("_")


def build_fab_qtime_files_fast(
    event_path: str,
    feature_store: Path,
    key_cols: list[str],
    step_col: str = "step_desc",
    ppid_col: str = "ppid",
    a_time_col: str = "A_TIME",
    b_time_col: str = "B_TIME",
    c_time_col: str = "C_TIME",
    save_csv: bool = False,
    exclude_step_desc: str = "AAA",
    exclude_ppid: str = "BBBB",
):
    lf = pl.scan_parquet(event_path)

    schema = lf.collect_schema()

    casts = []
    if schema[a_time_col] == pl.String:
        casts.append(pl.col(a_time_col).str.strptime(pl.Datetime, strict=False).alias(a_time_col))
    if schema[b_time_col] == pl.String:
        casts.append(pl.col(b_time_col).str.strptime(pl.Datetime, strict=False).alias(b_time_col))
    if schema[c_time_col] == pl.String:
        casts.append(pl.col(c_time_col).str.strptime(pl.Datetime, strict=False).alias(c_time_col))

    # 1) 필요한 컬럼만
    # 2) 제외 조건 적용
    # 3) C_TIME 최신 row 1개 선택
    # 4) 초 단위 절대값 QTIME 계산
    latest_df = (
        lf
        .select(key_cols + [step_col, ppid_col, a_time_col, b_time_col, c_time_col])
        .with_columns(casts)
        .filter(
            pl.col(step_col).is_not_null() &
            pl.col(a_time_col).is_not_null() &
            pl.col(b_time_col).is_not_null() &
            pl.col(c_time_col).is_not_null()
        )
        .filter(
            ~(
                (pl.col(step_col) == exclude_step_desc) &
                (pl.col(ppid_col) == exclude_ppid)
            )
        )
        .sort(by=key_cols + [step_col, c_time_col])
        .group_by(key_cols + [step_col], maintain_order=True)
        .agg(
            pl.col(a_time_col).last().alias(a_time_col),
            pl.col(b_time_col).last().alias(b_time_col),
        )
        .with_columns(
            (
                (pl.col(a_time_col) - pl.col(b_time_col))
                .dt.total_seconds()
                .abs()
            ).alias("QTIME")
        )
        .select(key_cols + [step_col, "QTIME"])
        .collect()
    )

    print(f"[INFO] rows after latest selection: {latest_df.height:,}")

    # collect 한 번만 하고, 메모리에서 step별 분할
    parts = latest_df.partition_by(step_col, as_dict=True)

    print(f"[INFO] total step_desc count: {len(parts):,}")

    for step_key, df_step in parts.items():
        step_value = step_key[0] if isinstance(step_key, tuple) else step_key
        step_safe = safe_name(step_value)
        qtime_col = f"FAB_{step_safe}_QTIME"

        out_df = df_step.select(key_cols + [pl.col("QTIME").alias(qtime_col)])

        parquet_file = feature_store / f"FAB_{step_safe}_QTIME.parquet"
        out_df.write_parquet(parquet_file, compression="lz4")

        if save_csv:
            csv_file = feature_store / f"FAB_{step_safe}_QTIME.csv"
            out_df.write_csv(csv_file)

        print(f"[SAVE] {parquet_file}")
        if save_csv:
            print(f"[SAVE] {csv_file}")


build_fab_qtime_files_fast(
    event_path=EVENT_PATH,
    feature_store=FEATURE_STORE,
    key_cols=KEY_COLS,
    step_col=STEP_COL,
    ppid_col=PPID_COL,
    a_time_col=A_TIME_COL,
    b_time_col=B_TIME_COL,
    c_time_col=C_TIME_COL,
    save_csv=SAVE_CSV,
    exclude_step_desc=EXCLUDE_STEP_DESC,
    exclude_ppid=EXCLUDE_PPID,
)

