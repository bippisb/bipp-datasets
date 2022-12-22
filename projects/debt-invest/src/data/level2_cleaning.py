from pathlib import Path

import pandas as pd

dir_path = Path.cwd()
raw_data_path = Path.joinpath(dir_path, "data", "raw")
interim_data_path = Path.joinpath(dir_path, "data", "interim")

df = pd.read_stata(Path.joinpath(raw_data_path, "level2.dta"))
df.columns

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
    print("Common ID is unique for Level 2")
else:
    print("Common ID not unique for Level 2")

head_rel_labels = {
    1: "self",
    2: "spouse of head",
    3: "married child",
    4: "spouse of married child",
    5: "unmarried child",
    6: "grand child",
    7: "father/mother/ father-in-law/ mother-in-law",
    8: "brother / sister/ brother-in-law/ sister-in-law/other relatives",
    9: "servant/employees/ other non-relatives",
}

gender_labels = {1: "male", 2: "female", 3: "transgenderl"}

education_labels = {
    1: "not literate",
    2: "literate: below primary",
    3: "primary",
    4: "upper primary/middle",
    5: "secondary",
    6: "higher secondary",
    7: "diploma /certificate course (upto secondary)",
    8: "diploma/certificate course (higher secondary)",
    10: "diploma/certificate course(graduation & above)",
    11: "graduate",
    12: "post graduate and above",
}

bank_labels = {
    1: "yes with banking services taken only from bank branch",
    2: "yes with banking services taken only from bank mitra",
    3: "yes with banking services taken from bank branch & bank mitra",
    4: "no account",
}


df["relation_to_head"] = df["relation_to_head"].map(head_rel_labels)
df["gender"] = df["gender"].map(gender_labels)
df["highest_edu_level_attained"] = df["highest_edu_level_attained"].map(
    education_labels
)
df["deposit_acc_in_cb_rrb_coop"] = df["deposit_acc_in_cb_rrb_coop"].map(
    bank_labels
)


csv_path = Path.joinpath(interim_data_path, "level2.csv")
df.to_csv(csv_path, index=False)
