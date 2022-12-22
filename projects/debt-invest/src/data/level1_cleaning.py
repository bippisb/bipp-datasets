from pathlib import Path

import pandas as pd

dir_path = Path.cwd()
raw_data_path = Path.joinpath(dir_path, "data", "raw")
interim_data_path = Path.joinpath(dir_path, "data", "interim")

df = pd.read_stata(Path.joinpath(raw_data_path, "level1.dta"))

df.drop(
    [
        "FSU_Serial_No",
        "Round",
        "Schedule",
        "Sample",
        "NSS_Region",
        "Stratum",
        "Sub_Stratum",
        "Sub_Round",
        "FOD_Sub_Region",
        "Second_stage_stratum_no",
        "Sample_hhld_No",
        "Visit_number",
        "Level",
        "Filler",
        "Blank",
        "NSC",
    ],
    axis=1,
    inplace=True,
)


var_names = [x.lower() for x in df.columns]
df.columns = var_names


df1 = df[["common_id", "sector", "state", "district", "multiplier", "w"]]

if df1["common_id"].is_unique:
    print("Common ID is unique for Level 1")
else:
    print("Common ID not unique for Level 1")

csv_path = Path.joinpath(interim_data_path, "level1.csv")
df1.to_csv(csv_path, index=False)
