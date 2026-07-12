import polars as pl
import glob
import os
import re
import spotfire.sbdf as sbdf

def sort_by_step(cols, prefix):
    def step_key(col):
        # prefix 뒤 문자열
        part = col.split(prefix, 1)[-1]

        # 첫 숫자 (정수/소수) 찾기
        m = re.search(r'\d+(?:\.\d+)?', part)

        if m:
            return float(m.group())
        else:
            return float("inf")  # 숫자 없으면 맨 뒤

    return sorted(cols, key=step_key)

# 아래 리팩토링
PATH = "DB/"
SAVE_PATH = "DB/4.SEND_FORM"

os.makedirs(SAVE_PATH, exist_ok=True)
os.makedirs(os.path.join(SAVE_PATH, "0.KNOB"), exist_ok=True)
os.makedirs(os.path.join(SAVE_PATH, "1.FAB"), exist_ok=True)
os.makedirs(os.path.join(SAVE_PATH, "2.VM"), exist_ok=True)
os.makedirs(os.path.join(SAVE_PATH, "3.INLINE"), exist_ok=True)

# -------------------------------------------------
# 1️⃣ parquet 파일 찾기
# -------------------------------------------------
files = sorted(glob.glob(os.path.join(PATH, "ML_TABLE*.parquet")))

if not files:
    raise FileNotFoundError("ML_TABLE*.parquet 파일이 없습니다.")

print("읽는 파일 목록:")
for f in files:
    print(" -", f)

# -------------------------------------------------
# 2️⃣ Lazy 병합 (메모리 거의 안씀)
# -------------------------------------------------
lazy_dfs = [pl.scan_parquet(f) for f in files]

df = pl.concat(lazy_dfs, how="diagonal_relaxed")

# -------------------------------------------------
# 3️⃣ wafer 중복 제거 (최신 파일 우선)
# -------------------------------------------------
KEY = ["PRODUCT", "ROOT_LOT_ID", "WAFER_ID"]

df = df.unique(subset=KEY, keep="last")

# 아직도 메모리에 안올라옴 (중요)

# -------------------------------------------------
# 4️⃣ 컬럼 그룹 분리
# -------------------------------------------------
cols = df.collect_schema().names()

def pick(prefix):
    return [c for c in cols if c.startswith(prefix)]

knob_cols   = sort_by_step(pick("KNOB_"), "KNOB_")
mask_cols = sort_by_step(pick("MASK_"), "MASK_")
fab_cols    = sort_by_step(pick("FAB_"), "FAB_")
inline_cols = sort_by_step(pick("INLINE_"), "INLINE_")
vm_cols     = sort_by_step(pick("VM_"), "VM_")

print(f"KNOB+MASK  : {len(knob_cols+mask_cols)}")
print(f"FAB   : {len(fab_cols)}")
print(f"INLINE: {len(inline_cols)}")
print(f"VM    : {len(vm_cols)}")

# -------------------------------------------------
# 5️⃣ 각 블록 생성 (Lazy 상태 유지)
# -------------------------------------------------
key_cols = KEY

knob_df   = df.select(key_cols + knob_cols + mask_cols)
fab_df    = df.select(key_cols + fab_cols)
inline_df = df.select(key_cols + inline_cols)
vm_df     = df.select(key_cols + vm_cols)

# -------------------------------------------------
# 6️⃣ 저장 (여기서 처음으로 실제 계산 발생)
# streaming=True 가 매우 중요
# -------------------------------------------------
knob_df.collect(streaming=True).write_csv(
    os.path.join(SAVE_PATH, "0.KNOB/KNOB_ML_TABLE.csv")
)

fab_df.collect(streaming=True).write_csv(
    os.path.join(SAVE_PATH, "1.FAB/FAB_ML_TABLE.csv")
)

vm_df.collect(streaming=True).write_csv(
    os.path.join(SAVE_PATH, "2.VM/VM_ML_TABLE.csv")
)

inline_df.collect(streaming=True).write_csv(
    os.path.join(SAVE_PATH, "3.INLINE/INLINE_ML_TABLE.csv")
)

knob_df_pandas = knob_df.collect().to_pandas()
fab_df_pandas = fab_df.collect().to_pandas()
vm_df_pandas = vm_df.collect().to_pandas()
inline_df_pandas = inline_df.collect().to_pandas()

sbdf.export_data(knob_df_pandas, "DB/4.SEND_FORM/0.KNOB/KNOB_ML_TABLE.sbdf")
sbdf.export_data(fab_df_pandas, "DB/4.SEND_FORM/1.FAB/FAB_ML_TABLE.sbdf")
sbdf.export_data(vm_df_pandas, "DB/4.SEND_FORM/2.VM/VM_ML_TABLE.sbdf")
sbdf.export_data(inline_df_pandas, "DB/4.SEND_FORM/3.INLINE/INLINE_ML_TABLE.sbdf")

print("\nDB 생성 완료")
