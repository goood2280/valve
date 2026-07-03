import sys
import yaml
import polars as pl
import pandas as pd
from pathlib import Path
from functools import lru_cache

def load_config(name: str, path="config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    if name not in cfg:
        raise ValueError(f"{name} not found in config")

    return cfg[name]

vehicle_name = sys.argv[1]  
config = load_config(vehicle_name, f"config.yaml")
globals().update(config)

# ==============================================
# 설정
# ==============================================
EVENT_ROOT = Path(rf'D:\DB\2.EVENT_DB\{vehicle}')
# EVENT_ROOT = Path("DB/2.EVENT_DB") / vehicle
KEY_COLS = ["root_lot_id", "wafer_id"]
FEATURE_STORE = Path(rf'D:\DB\3.FEATURE_STORE\{vehicle}') 
# FEATURE_STORE = Path("DB/3.FEATURE_STORE") / vehicle
FEATURE_STORE.mkdir(parents=True, exist_ok=True)
KNOB_PATH = f"KNOB/ppid_knob.csv"
# special_steps = ["AAA"] 
special_steps = []
# ==============================================
# 1️⃣ EVENT 데이터 로드 (Lazy로 전체 스캔)
# ==============================================
event_files = list(EVENT_ROOT.glob("**/*.parquet"))

if not event_files:
    raise ValueError("EVENT 파일이 없습니다.")

print(f"{len(event_files)} event files found")

event = pl.scan_parquet(event_files)

# 필요한 컬럼만 선택 (메모리 절약)
event = event.select([
    "root_lot_id",
    "wafer_id",
    "step_desc",
    "ppid",
    "tkout_time"
])

special = (
    event
    .filter(pl.col("step_desc").is_in(special_steps))
    .sort("tkout_time")
    .group_by(KEY_COLS + ["step_desc"])
    .last()
)

# special을 normal로 변경
normal = (
    event
    .filter(~pl.col("step_desc").is_in(special_steps))
    .filter(
        pl.col("ppid").is_not_null() &
        ~pl.col("ppid").str.contains("PPID1")
    )
    .sort("tkout_time")
    .group_by(KEY_COLS + ["step_desc"])
    .last()
)

event_last = pl.concat([normal, special]).collect()

# step_desc 이름 자체를 컬럼 이름으로 사용
wafer_matrix = event_last.pivot(
    values="ppid",
    index=KEY_COLS,
    columns="step_desc"
)

# 컬럼 이름에 _PPID 붙이기
rename_map = {
    c: f"{c}_PPID" for c in wafer_matrix.columns if c not in KEY_COLS
}
wafer_matrix = wafer_matrix.rename(rename_map)

wafer_matrix = wafer_matrix.with_columns(
    pl.lit(vehicle_name).alias("PRODUCT")
)
if 'BBB' not in wafer_matrix.columns:
    wafer_matrix = wafer_matrix.with_columns(pl.lit(None).alias("BBB"))

out_csv = "wafer_ppid_matrix.csv"
# wafer_matrix 전체 저장
wafer_matrix.head(50).write_csv(out_csv)

# 원본 컬럼명 → 대문자 매핑 저장
orig_cols = wafer_matrix.columns
col_map_upper = {c.upper(): c for c in orig_cols}

rules = pl.read_csv(KNOB_PATH)
wafer_df = wafer_matrix.rename({c: c.upper() for c in wafer_matrix.columns}).to_pandas()
feature_map = {f.upper(): f for f in rules["feature_name"].unique()}

# rules는 Pandas로 변환
rules_pd = rules.to_pandas()

rules_pd["feature_name"] = rules_pd["feature_name"].str.upper()
rules_pd["step_desc"] = rules_pd["step_desc"].astype(str).str.upper().str.strip()

rules = rules_pd.copy()

# 문자열 정리 (공백/NaN 방지)
rules["feature_name"] = rules["feature_name"].astype(str).str.strip()
rules["step_desc"] = rules["step_desc"].astype(str).str.strip()

# 모든 feature 목록
all_features = set(rules["feature_name"].unique())

# -------------------------------------------------
# 1️⃣ dependency 생성
# desc 가 "_ABC"로 끝나면 ABC feature를 부모로 인식
# -------------------------------------------------
SUFFIX = "_Split"   # ★ 여기서만 규칙 관리

dependency = {f: set() for f in all_features}

# 빠르게 찾기 위해 대문자 기준으로 매핑
feature_lookup = {f.upper(): f for f in all_features}

for _, row in rules.iterrows():
    feature = row["feature_name"]
    desc = str(row["step_desc"]).upper()

    # 모든 feature_name이 desc 안에 있는지 검사
    for fname_upper, real_name in feature_lookup.items():

        # 자기 자신은 제외
        if real_name == feature:
            continue

        # desc 안에 해당 feature가 등장하면 의존성 추가
        if fname_upper in desc:
            dependency[feature].add(real_name)

# -------------------------------------------------
# 2️⃣ 순환참조 검사
# -------------------------------------------------
def detect_cycle(dep):
    visiting = set()
    visited = set()

    def dfs(f):
        if f in visiting:
            raise ValueError(f"❌ Feature 순환참조 발생: {f}")
        if f in visited:
            return
        visiting.add(f)
        for p in dep[f]:
            dfs(p)
        visiting.remove(f)
        visited.add(f)

    for f in dep:
        dfs(f)

detect_cycle(dependency)

# -------------------------------------------------
# 3️⃣ depth 자동 계산
# -------------------------------------------------
@lru_cache(None)
def calc_depth(feature):
    parents = dependency.get(feature, set())
    if not parents:
        return 0
    return 1 + max(calc_depth(p) for p in parents)

feature_depth_map = {f: calc_depth(f) for f in all_features}

# -------------------------------------------------
# 4️⃣ features 순서 자동 정렬 (★ 핵심)
# -------------------------------------------------
features = sorted(all_features, key=lambda x: feature_depth_map[x])

# -------------------------------------------------
# 5️⃣ 저장 대상 (기존 로직 유지)
# -------------------------------------------------
features_to_store = (
    rules_pd[rules_pd["use"] == True]["feature_name"]
    .unique()
    .tolist()
)

# --------------------------------------------------
# 2️⃣ 조건 평가 함수
# --------------------------------------------------
def build_condition(df, step, op, val):
    step = step.strip().upper()

    if step in df.columns:
        col = step
    elif f"{step}_PPID" in df.columns:
        col = f"{step}_PPID"
    else:
        return pd.Series(False, index=df.index)

    s = df[col].astype(str)

    if op == "eq":
        return s == val
    elif op == "neq":
        return s != val
    elif op == "contains":
        return s.str.contains(val, na=False)
    elif op == "in":
        return s.isin(val.split("|"))
    elif op == "not_in":
        return ~s.isin(val.split("|"))
    elif op == "_null":
        return df[col].isna()
    elif op == "not_null":
        return df[col].notna()
    else:
        return pd.Series(True, index=df.index)

# --------------------------------------------------
# 3️⃣ Feature 생성 루프
# --------------------------------------------------

total = len(features)

for idx, feature in enumerate(features, start=1):
 
    depth = feature_depth_map.get(feature, "NA")
    percent = (idx / total) * 100
    remaining = total - idx

    print(f"[{idx}/{total}] ({percent:6.2f}%) | [Depth {depth}] {feature} 생성중")

    f_rules = rules_pd[rules_pd["feature_name"] == feature]

    wafer_df[feature] = None

    # -------- 일반 Rule --------
    normal_rules = f_rules[f_rules["rule_order"] != "RO"].copy()
    normal_rules["rule_num"] = normal_rules["rule_order"].str.extract(r'(\d+)').astype(int)
    normal_rules = normal_rules.sort_values("rule_num")

    for rule_num in normal_rules["rule_num"].unique():
        block = normal_rules[normal_rules["rule_num"] == rule_num]
        category = block["category"].iloc[0]

        cond = pd.Series(True, index=wafer_df.index)
        for _, row in block.iterrows():
            cond &= build_condition(wafer_df, row["step_desc"], row["operator"], row["value"])

        wafer_df.loc[wafer_df[feature].isna() & cond, feature] = category

    # -------- RO 처리 --------
    ro_rules = f_rules[f_rules["rule_order"] == "RO"]
    
    if not ro_rules.empty:
        # step_desc 정리
        ro_steps = [
            s.strip().upper()
            for s in ro_rules["step_desc"].dropna()
            if isinstance(s, str) and s.strip() != "" and s.lower() != "nan"
        ]
    
        cols = []
    
        for step in ro_steps:
            if step in wafer_df.columns:
                cols.append(wafer_df[step].astype(str))
            elif f"{step}_PPID" in wafer_df.columns:
                cols.append(wafer_df[f"{step}_PPID"].astype(str))
    
        # 🔹 유효 step 컬럼이 있는 경우 → signature
        if len(cols) > 0:
            signature = cols[0]
            for c in cols[1:]:
                signature = signature + "_" + c
    
            wafer_df.loc[wafer_df[feature].isna(), feature] = signature
    
        # 🔹 step_desc가 없거나 전부 무효 → category 기본값
        else:
            default_cat = ro_rules["category"].iloc[0]
            wafer_df.loc[wafer_df[feature].isna(), feature] = default_cat

    # -------- Feature Store 저장 --------
    if feature not in features_to_store:
        continue

    key_cols_upper = [c.upper() for c in KEY_COLS]
    feature_upper = feature.upper()
    
    save_df = wafer_df[key_cols_upper + [feature_upper]].copy()
    # save_df.columns = [
    #     col_map_upper.get(c, feature_map.get(c, c))
    #     for c in save_df.columns
    # ]
    save_df.columns = [
        f"KNOB_{feature_map.get(c, c)}" if c == feature_upper
        else col_map_upper.get(c, c)
        for c in save_df.columns
    ]

    safe_feature = feature.replace("/", "_DIV_")
    save_df.to_parquet(FEATURE_STORE / f"KNOB_{safe_feature}.parquet", index=False)
    # save_df.to_csv(FEATURE_STORE / f"KNOB_{feature}.csv", index=False)

print("모든 feature 생성 완료")

print("Depth 기반 Rule Engine 완료")
