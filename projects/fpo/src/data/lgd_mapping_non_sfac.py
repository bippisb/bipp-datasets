# Importing necessary modules

from pathlib import Path

import pandas as pd
from fuzzywuzzy import process

# Creating variables for directory paths

dir_path = Path.cwd()
raw_data_path = Path.joinpath(dir_path, "data", "raw")
interim_data_path = Path.joinpath(dir_path, "data", "interim")
processed_data_path = Path.joinpath(dir_path, "data", "processed")
ext_data_path = Path.joinpath(dir_path, "data", "external")

# Reading and processing LGD mapper file

lgd = pd.read_csv(Path.joinpath(ext_data_path, "lgd_district.csv"))

lgd.drop(
    ["St_Cs2011_code", "St_Cs2001_code", "Dt_Cs2011_code", "Dt_Cs2001_code"],
    axis=1,
    inplace=True,
)

lgd = lgd.rename(
    columns={
        "State Name(In English)": "state",
        "District Name(In English)": "district",
    }
)
# Creating state-district LGD key column

lgd["state_dist"] = ""
for i in range(0, 734):
    lgd["state_dist"][i] = lgd["state"][i].rstrip() + lgd["district"][i]

# Reading Non-SFAC CSV file into dataframe

non_sfac = pd.read_csv(Path.joinpath(processed_data_path, "non_sfac.csv"))

# Creating state-district data key column for LGD mapping

non_sfac["state"] = non_sfac["state"].str.upper()
non_sfac["district"] = non_sfac["district"].str.upper()
non_sfac["state_dist"] = non_sfac["state"] + non_sfac["district"]

# Merging Non-SFAC FPO dataframe and LGD mapper dataframe

df = pd.merge(
    non_sfac,
    lgd,
    how="outer",
    left_on="state_dist",
    right_on="state_dist",
    validate="m:1",
    indicator=True,
    suffixes=["_DATA", "_LGD"],
)

# FUZZY MATCHING

# Obtain unmapped values for fuzzy matching

not_lgd_mapped = df[(df["_merge"] == "left_only")][
    [
        "state_DATA",
        "district_DATA",
        "state_dist",
    ]
]
not_lgd_mapped = not_lgd_mapped.drop_duplicates(subset="state_dist")
not_lgd_mapped.drop([45], axis=0, inplace=True)

# Create mapper file for replacing data state-district keys

result = [
    process.extractOne(i, lgd["state_dist"]) for i in not_lgd_mapped["state_dist"]
]
result = pd.DataFrame(result, columns=["match", "score", "id"])
result.drop("id", axis=1, inplace=True)

not_lgd_proxy_df = (
    pd.DataFrame(not_lgd_mapped["state_dist"], index=None)
    .reset_index()
    .drop("index", axis=1)
)

mapper_df = pd.concat(
    [not_lgd_proxy_df, result],
    axis=1,
    ignore_index=True,
    names=["original", "match", "score"],
)

mapper_df = mapper_df[mapper_df[2] >= 90]
mapper_dict = dict(zip(mapper_df[0], mapper_df[1]))
non_sfac["state_dist"] = non_sfac["state_dist"].replace(mapper_dict)

# Merging fuzzy mapped dataframe and lgd mapper dataframe

df = pd.merge(
    non_sfac,
    lgd,
    how="outer",
    left_on="state_dist",
    right_on="state_dist",
    validate="m:1",
    indicator=True,
    suffixes=["_SFAC", "_LGD"],
)

# Iterating the fuzzy mapping and matching process

not_lgd_mapped = df[(df["_merge"] == "left_only")][
    [
        "state_SFAC",
        "district_SFAC",
        "state_dist",
    ]
]
not_lgd_mapped = not_lgd_mapped.drop_duplicates(subset="state_dist")
not_lgd_mapped.drop([45], axis=0, inplace=True)

result = [
    process.extractOne(i, lgd["state_dist"]) for i in not_lgd_mapped["state_dist"]
]
result = pd.DataFrame(result, columns=["match", "score", "id"])
result.drop("id", axis=1, inplace=True)

not_lgd_proxy_df = (
    pd.DataFrame(not_lgd_mapped["state_dist"], index=None)
    .reset_index()
    .drop("index", axis=1)
)

mapper_df = pd.concat(
    [not_lgd_proxy_df, result],
    axis=1,
    ignore_index=True,
    names=["original", "match", "score"],
)

mapper_df = mapper_df[mapper_df[2] >= 90]
mapper_dict = dict(zip(mapper_df[0], mapper_df[1]))
non_sfac["state_dist"] = non_sfac["state_dist"].replace(mapper_dict)

df = pd.merge(
    non_sfac,
    lgd,
    how="outer",
    left_on="state_dist",
    right_on="state_dist",
    validate="m:1",
    indicator=True,
    suffixes=["_SFAC", "_LGD"],
)

not_lgd_mapped = df[(df["_merge"] == "left_only")][
    [
        "state_SFAC",
        "district_SFAC",
        "state_dist",
    ]
]
not_lgd_mapped = not_lgd_mapped.drop_duplicates(subset="state_dist")

not_lgd_mapped.to_csv("non_sfac_unmapped.csv")

# Processing the final mapped dataframe

df1 = df[df["_merge"] == "both"]

df1 = df1.drop(["_merge", "district_LGD", "state_LGD"], axis=1)

cols = [
    "state_SFAC",
    "district_SFAC",
    "state_dist",
    "St_LGD_code",
    "Dt_LGD_code",
    "fpo_name",
    "legal_form",
    "reg_no",
    "address",
    "contact_details",
    "major_crop_names",
    "regn_date",
]

df1 = df1[cols]

df1 = df1.rename(columns={"state_SFAC": "state", "district_SFAC": "district"})

df1.drop(["state_dist"], axis=1, inplace=True)
df1.rename(
    columns={"St_LGD_code": "state_lgd_code", "Dt_LGD_code": "district_lgd_code"},
    inplace=True,
)

# Save final dataframe to CSV file

df1.to_csv(Path.joinpath(processed_data_path, "non_sfac_lgd.csv"), index=False)
