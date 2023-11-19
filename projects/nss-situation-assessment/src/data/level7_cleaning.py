from pathlib import Path

import pandas as pd

dir_path = Path.cwd()
raw_data_path = Path.joinpath(dir_path, "data", "raw")
interim_data_path = Path.joinpath(dir_path, "data", "interim")
ext_data_path = Path.joinpath(dir_path, "data", "external")

df = pd.read_stata(Path.joinpath(raw_data_path, "level7.dta"))

df.drop(
    [
        "Centre_code_Round",
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
        "Blank",
        "Level",
        "Filler",
        "NSC",
    ],
    axis=1,
    inplace=True,
)

var_names = [x.lower() for x in df.columns]
df.columns = var_names

if df["common_id"].is_unique:
    print("Common ID is unique for Level 7")
else:
    print("Common ID not unique for Level 7")

labels = pd.read_csv(Path.joinpath(ext_data_path, "crop_code_labels.csv"))

crop_list = list(labels["crop"])
code_list = list(labels["code"])

crop_labels = {}

for i in range(0, len(code_list)):
    c = crop_list[i].split(".")
    cr = c[0].split(" ")
    crop_labels[code_list[i]] = cr[0]

df["crop_code"] = df["crop_code"].map(crop_labels)

csv_path = Path.joinpath(interim_data_path, "level7.csv")
df.to_csv(csv_path, index=False)
