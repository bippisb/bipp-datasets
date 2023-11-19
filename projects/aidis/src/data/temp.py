from pathlib import Path

import pandas as pd

# defining paths
dir_path = Path.cwd()
raw_data_path = Path.joinpath(dir_path, "data", "raw")
interim_data_path = Path.joinpath(dir_path, "data", "interim")

df = pd.read_stata(Path.joinpath(raw_data_path, "level6.dta"))

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

if df["common_id"].is_unique:
    print("Common ID is unique for Level 6")
else:
    print("Common ID not unique for Level 6")

sl_no_labels = {
    97: "total urban land outside the FSU",
    98: "total homestead land owned",
    99: "total land owned",
}

type_of_land_labels = {
    1: "crop area, irrigated/unirrigated",
    2: "other area for agricultural/farm business",
    3: "for non-farm business",
    10: "residential area including homestead",
    9: "other areas",
}
female_members_share_labels = {1: "yes", 2: "no"}
df["serial_noof_plot"] = df["serial_noof_plot"].map(sl_no_labels)

print(df[["serial_noof_plot", "type_of_land_code"]])

print(df.columns)
