from pathlib import Path

import pandas as pd

dir_path = Path.cwd()
raw_data_path = Path.joinpath(dir_path, "data", "raw")
interim_data_path = Path.joinpath(dir_path, "data", "interim")


df = pd.read_stata(Path.joinpath(raw_data_path, "level7.dta"))

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
    print("Common ID is unique for Level 7")
else:
    print("Common ID not unique for Level 7")

df["building_type"] = df["serial_no"]

building_labels = {
    1: "residential building - used as dwelling by household members",
    2: "residential building - other residential building within the village/town",
    3: "residential building - other residential building outside the village/town",
    4: "building used for farm business - animal shed",
    5: "building used for farm business - others such as barn, warehouse (incl. cold storage), farm house, etc",
    6: "building used for non-farm business(workplace, workshop, mfg. unit, shop, etc.)",
    7: "building for other purposes (charitable, recreational like cinema hall, temple etc.)",
    8: "work-in-progress (structure under construction)",
    9: "other constructions (well, borewell, tubewell, field distribution system, etc.)",
    10: "total",
}

df["building_type"] = df["building_type"].map(building_labels)

csv_path = Path.joinpath(interim_data_path, "level7.csv")
df.to_csv(csv_path, index=False)
