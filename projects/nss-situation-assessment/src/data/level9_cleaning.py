from pathlib import Path

import pandas as pd

dir_path = Path.cwd()
raw_data_path = Path.joinpath(dir_path, "data", "raw")
interim_data_path = Path.joinpath(dir_path, "data", "interim")

df = pd.read_stata(Path.joinpath(raw_data_path, "level9.dta"))

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
        "Blank",
        "NSC",
    ],
    axis=1,
    inplace=True,
)

var_names = [x.lower() for x in df.columns]
df.columns = var_names

if df["common_id"].is_unique:
    print("Common ID is unique for Level 9")
else:
    print("Common ID not unique for Level 9")

df["animal_category"] = df["srl_no"]

cat_labels = {
    1: "cattle in-milk",
    2: "cattle young stock",
    3: "cattle other",
    4: "buffalo in-milk",
    5: "buffalo young stock",
    6: "buffalo other",
    7: "ovine and other mammals (sheep, goat, pig, rabbits etc.)",
    8: "other large-heads (elephant, camel, horse, mule, pony, donkey, yak, mithun etc.) ",
    9: "poultry birds (hen, cock, chicken, duck, duckling, other poultry birds, etc.)",
    10: "total",
}

df["animal_category"] = df["animal_category"].map(cat_labels)

csv_path = Path.joinpath(interim_data_path, "level9.csv")
df.to_csv(csv_path, index=False)
