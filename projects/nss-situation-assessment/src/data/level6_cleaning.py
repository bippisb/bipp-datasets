from pathlib import Path

import pandas as pd

dir_path = Path.cwd()
raw_data_path = Path.joinpath(dir_path, "data", "raw")
interim_data_path = Path.joinpath(dir_path, "data", "interim")
ext_data_path = Path.joinpath(dir_path, "data", "external")

df = pd.read_stata(Path.joinpath(raw_data_path, "level6.dta"))

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
    print("Common ID is unique for Level 6")
else:
    print("Common ID not unique for Level 6")

labels = pd.read_csv(Path.joinpath(ext_data_path, "crop_code_labels.csv"))

crop_list = list(labels["crop"])
code_list = list(labels["code"])

crop_labels = {}

for i in range(0, len(code_list)):
    c = crop_list[i].split(".")
    cr = c[0].split(" ")
    crop_labels[code_list[i]] = cr[0]

unit_labels = {1: "kg", 2: "number"}
maj_disp_labels = {
    1: "local market (incl. local traders)",
    2: "APMC market",
    3: "input dealers",
    4: "cooperative",
    5: "Government agencies",
    6: "Farmer producer organisations (FPO)",
    7: "private processors",
    8: "contract farming sponsors/ companies",
    9: "others",
}
satisfactory_labels = {
    1: "satisfactory",
    2: "not satisfactory: lower than market price",
    3: "delayed payments",
    4: "deductions for loans borrowed",
    5: "faulty weighing and grading",
    9: "other cause of dissatisfaction",
}

print(len(pd.unique(df["unit_code"])))
print(len(pd.unique(df["major_disp_sold"])))
print(len(pd.unique(df["satisfied_sale_outcome"])))
print(len(pd.unique(df["crop_code"])))

df["crop_code"] = df["crop_code"].map(crop_labels)
df["unit_code"] = df["unit_code"].map(unit_labels)
df["major_disp_sold"] = df["major_disp_sold"].map(maj_disp_labels)
df["satisfied_sale_outcome"] = df["satisfied_sale_outcome"].map(satisfactory_labels)

csv_path = Path.joinpath(interim_data_path, "level6.csv")
df.to_csv(csv_path, index=False)
