from pathlib import Path

import pandas as pd

dir_path = Path.cwd()
raw_data_path = Path.joinpath(dir_path, "data", "raw")
interim_data_path = Path.joinpath(dir_path, "data", "interim")

df = pd.read_stata(Path.joinpath(raw_data_path, "level10.dta"))

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
        "blank",
        "NSC",
    ],
    axis=1,
    inplace=True,
)

var_names = [x.lower() for x in df.columns]
df.columns = var_names

if df["common_id"].is_unique:
    print("Common ID is unique for Level 10")
else:
    print("Common ID not unique for Level 10")

df["animal_farming_category"] = df["sl_no"]

animal_labels = {
    1: "milk (cattle) (litre)",
    2: "milk ( buffalo) (litre)",
    3: "milk (sheep goat, etc.) (litre)",
    4: "egg (poultry, duck, etc.) (no.)",
    5: "wool (sheep, etc.) (kg)",
    6: "fish (kg)",
    7: "livestock cattle (nos.)",
    8: "livestock buffalo (nos.)",
    9: " livestock sheep, goat, etc. (nos.)",
    10: "livestock pig (nos.)",
    11: "livestock poultry, duck, etc. (nos.)",
    12: "other livestock (nos.)",
    13: "skin, hide, bones",
    14: "manure",
    15: "value of other produce (Rs.)",
    16: "total value of produce",
}

df["animal_farming_category"] = df["animal_farming_category"].map(animal_labels)

csv_path = Path.joinpath(interim_data_path, "level10.csv")
df.to_csv(csv_path, index=False)
