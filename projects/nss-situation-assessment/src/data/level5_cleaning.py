from pathlib import Path

import pandas as pd

dir_path = Path.cwd()
raw_data_path = Path.joinpath(dir_path, "data", "raw")
interim_data_path = Path.joinpath(dir_path, "data", "interim")

df = pd.read_stata(Path.joinpath(raw_data_path, "level5.dta"))

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
    print("Common ID is unique for Level 5")
else:
    print("Common ID not unique for Level 5")

ind_joint_labels = {1: "individually", 2: "jointly"}
holding_type_labels = {
    1: "entirely owned",
    2: "entirely leased",
    3: "both owned and leased-in",
    4: "entirely otherwise posessed",
}
holding_use_labels = {
    1: "only for growing of crops: on land used for shifting /jhum cultivation",
    2: "only for growing of crops:on land other than the land used for shifting /jhum cultivation",
    3: " only for farming of animals",
    4: "both for crop growing and animal farming",
    5: "other agricultural uses",
}

df["operated_ind_jointly"] = df["operated_ind_jointly"].map(ind_joint_labels)
df["type_of_holding"] = df["type_of_holding"].map(holding_type_labels)
df["use_of_the_holding"] = df["use_of_the_holding"].map(holding_use_labels)

csv_path = Path.joinpath(interim_data_path, "level5.csv")
df.to_csv(csv_path, index=False)
