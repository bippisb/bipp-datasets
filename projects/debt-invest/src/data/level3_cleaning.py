from pathlib import Path

import pandas as pd

dir_path = Path.cwd()
raw_data_path = Path.joinpath(dir_path, "data", "raw")
interim_data_path = Path.joinpath(dir_path, "data", "interim")

df = pd.read_stata(Path.joinpath(raw_data_path, "level3.dta"))
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
    print("Common ID is unique for Level 3")
else:
    print("Common ID not unique for Level 3")

religion_labels = {
    1: "Hinduism ",
    2: "Islam",
    3: "Christianity",
    4: "Sikhism",
    5: "Jainism",
    6: "Buddhism",
    7: "Zoroastrianism",
    9: "other",
}


# In[9]:


social_group_labels = {
    1: "scheduled tribe (ST)",
    2: "scheduled caste (SC)",
    3: "other backward class (OBC)",
    9: "other",
}

household_rural_labels = {
    1: "self-employed in agriculture",
    2: "self-employed in non-agriculture",
    3: "regular wage/salary earning",
    4: "casual labour in agriculture",
    5: "casual labour in non-agriculture",
    9: "other",
}

household_urban_labels = {
    1: "self-employed",
    2: "regular wage/salary earning",
    3: "casual labour",
    9: "other",
}

yn_labels = {1: "Yes", 2: "No"}

df["religion"] = df["religion"].map(religion_labels)
df["social_group"] = df["social_group"].map(social_group_labels)
df["land_for_agri_last_365days"] = df["land_for_agri_last_365days"].map(
    yn_labels
)

for i in range(0, len(df["sector"])):
    if df["sector"][i] == 1:
        df["hh_type"][i] = household_rural_labels[df["hh_type"][i]]
    elif df["sector"][i] == 2:
        df["hh_type"][i] = household_urban_labels[df["hh_type"][i]]
    print(i, "done")

csv_path = Path.joinpath(interim_data_path, "level3.csv")
df.to_csv(csv_path, index=False)
