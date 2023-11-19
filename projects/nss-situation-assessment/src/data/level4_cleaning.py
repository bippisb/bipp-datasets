from pathlib import Path

import pandas as pd

dir_path = Path.cwd()
raw_data_path = Path.joinpath(dir_path, "data", "raw")
interim_data_path = Path.joinpath(dir_path, "data", "interim")

df = pd.read_stata(Path.joinpath(raw_data_path, "level4.dta"))

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
    print("Common ID is unique for Level 4")
else:
    print("Common ID not unique for Level 4")

df["categories_of_land"] = df["srl_no"]

cat_labels = {
    1: "land other than homestead owned and possessed",
    2: "land other than homestead leased-in and recorded",
    3: "land other than homestead leased-in and not recorded",
    4: "land other than homestead otherwise possessed",
    5: "land other than homestead leased-out",
    6: "homestead owned and possessed",
    7: "homestead leased-in and recorded",
    8: "homestead leased-in and not recorded",
    9: "homestead otherwise possessed",
    10: "total (homestead+other land)",
}

df["categories_of_land"] = df["categories_of_land"].map(cat_labels)

crop_labels = {
    1: "cereals",
    2: "pulses",
    4: "sugar crops",
    5: "condiments and spices",
    6: "fruits",
    7: "tuber crops",
    8: "vegetables",
    9: "other food crops",
    10: "oilseeds",
    11: "fibres",
    12: "dyes & tanning materials",
    13: "drugs & narcotics",
    14: "fodder crops",
    15: "plantation crops",
    16: "flower crops",
    17: "medicinal plants",
    18: "aromatic plants",
    19: "other non-food crops",
    20: "dairy",
    21: "poultry/duckery",
    22: "piggery",
    23: "fishery",
    29: "farming of other animals",
}

irri_labels = {
    1: "canal",
    2: "minor surface works (pond, tank, etc)",
    3: "ground water (tube well, well etc.)",
    4: "combination of canals, minor surface works and groundwater",
    9: "others",
}

tenure_labels = {
    1: "less than 6 months",
    2: "6 months or more but less than 1 year",
    3: "1 year or more but less than 2 years",
    4: "2 years or more",
}


df["crop_farming_code"] = df["crop_farming_code"].map(crop_labels)
df["irri_major_source"] = df["irri_major_source"].map(irri_labels)
df["irri_2nd_major_source"] = df["irri_2nd_major_source"].map(irri_labels)
df["tenure_of_lease"] = df["tenure_of_lease"].map(tenure_labels)

csv_path = Path.joinpath(interim_data_path, "level4.csv")
df.to_csv(csv_path, index=False)
