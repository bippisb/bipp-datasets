from pathlib import Path

import pandas as pd

dir_path = Path.cwd()
raw_data_path = Path.joinpath(dir_path, "data", "raw")
interim_data_path = Path.joinpath(dir_path, "data", "interim")
ext_data_path = Path.joinpath(dir_path, "data", "external")

df = pd.read_stata(Path.joinpath(raw_data_path, "level8.dta"))

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
    print("Common ID is unique for Level 8")
else:
    print("Common ID not unique for Level 8")

labels = pd.read_csv(Path.joinpath(ext_data_path, "crop_code_labels.csv"))

crop_list = list(labels["crop"])
code_list = list(labels["code"])

crop_labels = {}
for i in range(0, len(code_list)):
    c = crop_list[i].split(".")
    cr = c[0].split(" ")
    crop_labels[float(code_list[i])] = cr[0]

where_procure_labels = {
    1: "local market (incl. local traders)",
    2: "APMC market",
    3: "input dealers",
    4: "cooperative",
    5: "Government agencies",
    6: "Farmer producer organisations (FPO)",
    7: "private processors",
    8: "contract farming sponsors/ companies",
    10: "own farm",
    9: "others",
}
quality_labels = {1: "good", 2: "satisfactory", 3: "poor", 4: "dont know"}

df["input"] = df["sl_no"]
input_labels = {
    1: "seeds",
    2: "seeds",
    3: "seeds",
    4: "seeds",
    5: "seeds",
    6: "chemical fertilizers",
    7: "bio-fertilizers",
    8: "manures",
    9: "plant protection materials chemical",
    10: "plant protection materials bio-pesticides",
    11: "diesel",
    12: "electricity",
    13: "irrigation",
    14: "labour human",
    15: "labour animal",
    16: "minor repair and maintenance of machinery and equipment used in crop production",
    17: "interest on loans utilised for the purpose of crop production",
    18: "cost of hiring of machinery and equipment for crop production",
    19: "cost of crop insurance",
    20: "lease rent for land used for crop production",
    21: "other expenses for crop production",
    22: "total",
}

df["input"] = df["input"].map(input_labels)
df["inputs_from_where_procur"] = df["inputs_from_where_procur"].map(
    where_procure_labels
)
df["inputs_qual_adeq_code"] = df["inputs_qual_adeq_code"].map(quality_labels)
df["crop_code"] = df["crop_code"].map(crop_labels)

csv_path = Path.joinpath(interim_data_path, "level8.csv")
df.to_csv(csv_path, index=False)
