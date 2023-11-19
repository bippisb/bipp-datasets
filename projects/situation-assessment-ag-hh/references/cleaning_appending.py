from pathlib import Path

# import numpy as np
import pandas as pd

# defining paths
dir_path = Path.cwd()
raw_data_path = Path.joinpath(dir_path, "data", "raw")
interim_data_path = Path.joinpath(dir_path, "data", "interim")

"""the raw files are split into mutiple levels and each level need to cleaned individually based on the questionnaire """
"""appending will only happend after each leve is cleaned and ready for merge"""


"""**************LEVEL 1**********************"""
# BLOCK 1
# reading in level 1
df = pd.read_stata(Path.joinpath(raw_data_path, "level1.dta"))

# subsetting for necessary columns
df = df.drop(
    [
        "Centre_code_Round",
        "Round",
        "Schedule",
        "Sample",
        "NSS_Region",
        "Stratum",
        "Sub_Stratum",
        "Sub_Round",
        "FOD_Sub_Region",
        "Visit_no",
        "Level",
        "Filler",
        "Informant_Sl_No",
        "Response_Code",
        "Survey_Code",
        "Substitution_Casualty__Code",
        "Employee_code1",
        "Employee_code2",
        "Employee_code3",
        "Date_of_Survey",
        "Date_of_Despatch",
        "Time_to_canvass",
        "No_of_investigators",
        "Remarks",
        "Remarks1",
        "RemarkS2",
        "Remarks3",
        "Blank",
    ],
    axis=1,
)

# lower-casing all var names
var_names = [x.lower() for x in df.columns]
df.columns = var_names

# checking uniqueness of common_id
if df["common_id"].is_unique:
    print("Common ID is unique for Level 1")
else:
    print("Common ID not unique for Level 1")


# writing file to interim csv
csv_path = Path.joinpath(interim_data_path, "level1.csv")
df.to_csv(csv_path, index=False)

"""********************************************"""


"""**************LEVEL 2**********************"""
# This includes Block 3 where all demographic details of individuals of a household are listed.
# This info is not required for the project, hence omitted from cleaning and all further process.
"""********************************************"""


"""**************LEVEL 3**********************"""
# BLOCK 4


# reading in level 3
df = pd.read_stata(Path.joinpath(raw_data_path, "level3.dta"))

# subsetting for necessary columns
df = df.drop(
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
)


# lower-casing all var names
var_names = [x.lower() for x in df.columns]
df.columns = var_names

# checking uniqueness of common_id
if df["common_id"].is_unique:
    print("Common ID is unique for Level 3")
else:
    print("Common ID not unique for Level 3")


# adding categories to religion
religion_labels = {
    1: "Hinduism",
    2: "Islam",
    3: "Christianity",
    4: "Sikhism",
    5: "Jainism",
    6: "Buddhism",
    7: "Zorastrianism",
    9: "Others",
}
df["religion_code"] = df["religion_code"].map(religion_labels)


# adding categories to caste
caste_labels = {
    1: "Scheduled Tribe",
    2: "Scheduled Caste",
    3: "Other Backward Class",
    9: "Others",
}

df["social_group_code"] = df["social_group_code"].map(caste_labels)

# adding categories to hh_classification_code
hh_lables = {
    1: "Self-employment in: crop production",
    2: "Self-employment in: farming of animals",
    3: "Self-employment in: other agricultural activities",
    4: "Self-employment in: non-agricultural enterprise",
    5: "Regular wage/salaried earning in: agriculture",
    6: "Regular wage/salaried earning in: non-agriculture",
    7: "Casual labour in: agriculture",
    8: "Casual labour in:non-agriculture",
    9: "Others (pensioners, remittance,recipients, student, engaged in domestic duties, etc.)",
}

df["hh_classification_code"] = df["hh_classification_code"].map(hh_lables)


# adding categories to agr_production4000
prdn_4000_labels = {1: "Less than or equal to Rs.4000", 2: "More than Rs.4000"}
df["agr_production4000"] = df["agr_production4000"].map(prdn_4000_labels)


# adding categories to dwelling_unit_code
dwell_label = {1: "Owned", 2: "Hired", 3: "No dwelling unit", 9: "Others"}
df["dwelling_unit_code"] = df["dwelling_unit_code"].map(dwell_label)


# adding categories to type_of_structure
struct_label = {1: "Katcha", 2: "Semi-pucca", 3: "Pucca"}
df["type_of_structure"] = df["type_of_structure"].map(struct_label)


# writing file to interim csv
csv_path = Path.joinpath(interim_data_path, "level3.csv")
df.to_csv(csv_path, index=False)

"""********************************************"""
