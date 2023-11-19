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

# Read and process LGD mapper file

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

# Reading SFAC CSV file into dataframe

sfac = pd.read_csv(Path.joinpath(processed_data_path, "sfac.csv"))

# Manually correcting district names

for i in range(0, 898):
    sfac["district"][i] = sfac["district"][i].rstrip(",")
    if sfac["district"][i].upper() == "MEWAT":
        sfac["district"][i] = "NUH"
    if sfac["district"][i].upper() == "ALLAHABAD":
        sfac["district"][i] = "PRAYAGRAJ"
    if sfac["district"][i].upper() == "HAJIPUR":
        sfac["district"][i] = "VAISHALI"
    if sfac["district"][i].upper() == "BAKHTIYARPUR (PATNA)":
        sfac["district"][i] = "PATNA"
    if sfac["district"][i].upper() == "BARH (PATNA)":
        sfac["district"][i] = "PATNA"
    if sfac["district"][i].upper() == "EAST CHMAPARAN":
        sfac["district"][i] = "PURBI CHAMPARAN"
    if sfac["district"][i].upper() == "EAST CHAMPARAN":
        sfac["district"][i] = "PURBI CHAMPARAN"
    if sfac["district"][i].upper() == "MAGARLOAD (DHAMTARI)":
        sfac["district"][i] = "DHAMTARI"
    if sfac["district"][i].upper() == "GHUMANHERA":
        sfac["district"][i] = "SOUTH WEST"
    if sfac["district"][i].upper() == "NUVEM":
        sfac["district"][i] = "SOUTH GOA"
    if sfac["district"][i].upper() == "PERNEM":
        sfac["district"][i] = "NORTH GOA"
    if sfac["district"][i].upper() == "KUTCH":
        sfac["district"][i] = "KACHCHH"
    if sfac["district"][i].upper() == "DAHOD":
        sfac["district"][i] = "DOHAD"
    if sfac["district"][i].upper() == "GURGAON":
        sfac["district"][i] = "GURUGRAM"
    if sfac["district"][i].upper() == "JAMMU":
        sfac["state"][i] = "JAMMU AND KASHMIR"
    if sfac["district"][i].upper() == "NAGADI":
        sfac["district"][i] = "RANCHI"
    if sfac["district"][i].upper() == "GULBARGA, (KALABURAGI)":
        sfac["district"][i] = "KALABURAGI"
    if sfac["district"][i].upper() == "GULBARGA":
        sfac["district"][i] = "KALABURAGI"
    if sfac["district"][i].upper() == "BIJAPUR (VIJAYPUR)":
        sfac["district"][i] = "VIJAYAPURA"
    if sfac["district"][i].upper() == "SIRIGERE (CHITRADURGA)":
        sfac["district"][i] = "CHITRADURGA"
    if sfac["district"][i].upper() == "DIGRAS":
        sfac["district"][i] = "YAVATMAL"
    if sfac["district"][i].upper() == "BANKI":
        sfac["district"][i] = "CUTTACK"
    if sfac["district"][i].upper() == "BADAMBA":
        sfac["district"][i] = "CUTTACK"
    if sfac["district"][i].upper() == "BARAMBA":
        sfac["district"][i] = "CUTTACK"
    if sfac["district"][i].upper() == "TIGIRIA":
        sfac["district"][i] = "CUTTACK"
    if sfac["district"][i].upper() == "BAINA,HINJILICUT":
        sfac["district"][i] = "GANJAM"
    if sfac["district"][i].upper() == "HINJILICUT":
        sfac["district"][i] = "GANJAM"
    if sfac["district"][i].upper() == "PAKHOWAL":
        sfac["district"][i] = "LUDHIANA"
    if sfac["district"][i].upper() == "WEST SIKKIM":
        sfac["district"][i] = "WEST DISTRICT"
    if sfac["district"][i].upper() == "EAST SIKKIM":
        sfac["district"][i] = "EAST DISTRICT"
    if sfac["district"][i].upper() == "NORTH SIKKIM":
        sfac["district"][i] = "NORTH DISTRICT"
    if sfac["district"][i].upper() == "SOUTH SIKKIM":
        sfac["district"][i] = "SOUTH DISTRICT"
    if sfac["district"][i].upper() == "SRINAGAR":
        sfac["state"][i] = "JAMMU AND KASHMIR"
    if sfac["district"][i].upper() == "THOOTHUKUDI":
        sfac["district"][i] = "TUTICORIN"
    if sfac["district"][i].upper() == "BADKOT":
        sfac["district"][i] = "UTTARKASHI"

# Creating state-district data key column for LGD mapping

sfac["state"] = sfac["state"].str.upper()
sfac["district"] = sfac["district"].str.upper()
sfac["state_dist"] = sfac["state"] + sfac["district"]

# Merging SFAC FPO dataframe and LGD mapper dataframe

df = pd.merge(
    sfac,
    lgd,
    how="outer",
    left_on="state_dist",
    right_on="state_dist",
    validate="m:1",
    indicator=True,
    suffixes=["_SFAC", "_LGD"],
)

# FUZZY MATCHING

# Obtain unmapped values for fuzzy matching

not_lgd_mapped = df[(df["_merge"] == "left_only")][
    [
        "state_SFAC",
        "district_SFAC",
        "state_dist",
    ]
]

not_lgd_mapped = not_lgd_mapped.drop_duplicates(subset="state_dist")

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
sfac["state_dist"] = sfac["state_dist"].replace(mapper_dict)

# Merging fuzzy mapped dataframe and lgd mapper dataframe

df = pd.merge(
    sfac,
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

mapper_df = mapper_df[mapper_df[2] >= 85]
mapper_dict = dict(zip(mapper_df[0], mapper_df[1]))
sfac["state_dist"] = sfac["state_dist"].replace(mapper_dict)

df = pd.merge(
    sfac,
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
sfac["state_dist"] = sfac["state_dist"].replace(mapper_dict)


df = pd.merge(
    sfac,
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

not_lgd_mapped.to_csv("sfac_unmapped.csv")

# Processing the final mapped dataframe

df1 = df[df["_merge"] == "both"]

df1.drop(["_merge", "district_LGD", "state_LGD"], axis=1, inplace=True)

cols = [
    "state_SFAC",
    "district_SFAC",
    "state_dist",
    "St_LGD_code",
    "Dt_LGD_code",
    "programme",
    "resource_institution",
    "fpo_name",
    "legal_form",
    "reg_no",
    "address",
    "contact",
    "major_crops",
    "regn_date",
]

df1 = df1[cols]

df2 = df1.rename(columns={"state_SFAC": "state", "district_SFAC": "district"})

df2.to_csv(Path.joinpath(processed_data_path, "sfac_lgd.csv"), index=False)

data = pd.read_csv(Path.joinpath(processed_data_path, "sfac_lgd.csv"))

data.drop(["state_dist"], axis=1, inplace=True)
data.rename(
    columns={"St_LGD_code": "state_lgd_code", "Dt_LGD_code": "district_lgd_code"},
    inplace=True,
)

# Save SFAC FPO dataframe to CSV file

data.to_csv(Path.joinpath(processed_data_path, "sfac_lgd.csv"), index=False)
