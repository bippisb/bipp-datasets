from pathlib import Path

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

# reading in level 2
df = pd.read_stata(Path.joinpath(raw_data_path, "level2.dta"))

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
    print("Common ID is unique for Level 2")
else:
    print("Common ID not unique for Level 2")

# adding categories to relation to head
relation_to_head_lable = {
    1: "self",
    2: "spouse of head",
    3: "married child",
    4: "spouse of married child",
    5: "unmarried child",
    6: "grandchild",
    7: "father/mother/father-in-law/mother-in-law",
    8: "brother/sister/brother-inlaw/sister-in-law/other relatives",
    9: "servants/employees/other non-relatives",
}

# adding categories to Gender

gender_lable = {
    1: "male",
    2: "female",
    3: "transgender",
}

# adding categories to highest level of education

education_lable = {
    1: "not literate",
    2: "literate: below primary",
    3: "primary",
    4: "upper primary/middle",
    5: "secondary",
    6: "higher secondary",
    7: "diploma /certificate course (up to secondary)",
    8: "diploma/certificate course (higher secondary)",
    10: "diploma/certificate course(graduation & above)",
    11: "graduate",
    12: "post graduate and above",
}

# adding categories to status

status_lable = {
    11: "worked in h.h. enterprise (self-employed): own account worker",
    12: "employer",
    21: "worked as helper in h.h. enterprise (unpaid family worker)",
    31: "worked as regular salaried/ wage employee",
    41: "worked as casual wage labour: in public works other than MGNREG works",
    42: "in MGNREG works",
    51: "in other types of work",
    81: "did not work but was seeking and/or available for work",
    91: "attended educational institution",
    92: "attended domestic duties only",
    93: "attended domestic duties and was also engaged in free collection of goods (vegetables, roots, firewood, cattle feed, etc.),sewing, tailoring, weaving, etc. for household use",
    94: "rentiers, pensioners , remittance recipients, etc",
    95: "not able to work due to disability",
    97: "others (including begging, prostitution, etc.)",
}


# mapping the categories

df["relation_to_head"] = df["relation_to_head"].map(relation_to_head_lable)

df["gender"] = df["gender"].map(gender_lable)

df["highest_level_edu"] = df["highest_level_edu"].map(education_lable)

df["principal_activity_status_code"] = df["principal_activity_status_code"].map(
    status_lable
)

df["sub_activity_status_code"] = df["sub_activity_status_code"].map(status_lable)


# writing file to interim csv
csv_path = Path.joinpath(interim_data_path, "level2.csv")
df.to_csv(csv_path, index=False)


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


"""**************LEVEL 4**********************"""

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

df.rename(columns={"srl_no": "sr_no"}, inplace=True)

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

"""********************************************"""


"""**************LEVEL 5**********************"""
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

"""********************************************"""


"""**************LEVEL 6**********************"""
df = pd.read_stata(Path.joinpath(raw_data_path, "level6.dta"), preserve_dtypes=True)

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


# print(df[['sl_no', 'crop_code',]])


# if df["common_id"].is_unique:
# print("Common ID is unique for Level 6")
# else:
# print("Common ID not unique for Level 6")

crop_code_labels = {
    1: "Cereals",
    2: "Pulses",
    4: "Sugar",
    5: "Condiments",
    6: "Fruits",
    7: "Tuber",
    8: "Vegetables",
    10: "Oilseeds",
    11: "Fibres",
    12: "Dyes",
    13: "Drugs",
    14: "Fodder",
    15: "Plantation",
    16: "Flower",
    17: "Medicinal",
    18: "Aromatic",
    19: "Other",
    101: "paddy",
    102: "jowar",
    103: "bajra",
    104: "maize",
    105: "ragi",
    106: "wheat",
    107: "barley",
    108: "small millets",
    188: "other cereals",
    201: "gram",
    202: "tur",
    203: "urad",
    204: "moong",
    205: "masur",
    206: "horse",
    207: "beans",
    208: "peas",
    288: "other pulses",
    401: "sugarcane",
    402: "palmvriah",
    488: "other suger crops",
    501: "pepper",
    502: "chillies",
    503: "ginger",
    504: "turmeric",
    505: "cardamom(small)",
    506: "cardamom(large)",
    507: "betel",
    508: "garlic",
    509: "coriander",
    510: "tamarind",
    511: "cumin",
    512: "fennel",
    513: "nutmeg",
    514: "fenugreek",
    515: "cloves",
    516: "cinnamon",
    517: "cocoa",
    518: "kacholam",
    519: "betelvine",
    588: "Other condiments",
    601: "mangoes",
    602: "orange",
    603: "mosambi",
    604: "lemon",
    605: "othercitrous",
    606: "banana",
    607: "table",
    608: "wine",
    609: "apple",
    610: "pear",
    611: "peaches",
    612: "plum",
    613: "kiwi",
    614: "chiku",
    615: "papaya",
    616: "guava",
    617: "almond",
    618: "walnut",
    619: "cashewnuts",
    620: "apricot",
    621: "jackfruit",
    622: "lichi",
    623: "pineapple",
    624: "watermelon",
    625: "musk",
    626: "bread",
    627: "ber",
    628: "bel",
    629: "mulberry",
    630: "aonla",
    688: "other",
    701: "potato",
    702: "tapioca",
    703: "sweet",
    704: "yam",
    705: "elephant",
    706: "colocasia/arum",
    788: "other",
    801: "onion",
    802: "carrot",
    803: "radish",
    804: "beetroot",
    805: "turnip",
    806: "tomato",
    807: "spinach",
    808: "amaranths",
    809: "cabbage",
    810: "other",
    811: "brinjal",
    812: "peas",
    813: "lady's",
    814: "cauliflower",
    815: "cucumber",
    816: "bottle",
    817: "pumpkin",
    818: "bitter",
    819: "other",
    820: "guar",
    821: "beans",
    822: "drumstick",
    823: "green",
    888: "other",
    901: "other",
    1001: "groundnut",
    1002: "castorseed",
    1003: "sesamum",
    1004: "rapeseed",
    1005: "linseed",
    1006: "coconut",
    1007: "sunflower",
    1008: "safflower",
    1009: "soyabean",
    1010: "nigerseed",
    1011: "oil",
    1012: "toria",
    1088: "other",
    1101: "cotton",
    1102: "jute",
    1103: "mesta",
    1104: "sunhemp",
    1188: "other",
    1201: "indigo",
    1288: "other",
    1301: "opium",
    1302: "tobacco",
    1388: "other",
    1401: "guar",
    1402: "oats",
    1403: "green",
    1488: "other",
    1501: "tea",
    1502: "coffee",
    1503: "rubber",
    1588: "other",
    1601: "orchids",
    1602: "rose",
    1603: "gladiolus",
    1604: "carnation",
    1605: "marigold",
    1688: "other",
    1701: "asgandh",
    1702: "isabgol",
    1703: "sena",
    1704: "moosli",
    1788: "other",
    1801: "lemon",
    1802: "mint",
    1803: "menthol",
    1804: "eucalyptus",
    1888: "other",
    1901: "canes",
    1902: "bamboos",
    1988: "other",
}

# df["crop_name"] = df["sl_no"]
df["crop_name"] = df["crop_code"].map(crop_code_labels)
crop_name_col = df.pop("crop_name")
df.insert(6, "crop_name", crop_name_col)

print(df["crop_code"].isna().value_counts())
print(df["crop_name"].isna().value_counts())

print(df[df["crop_code"].isna()])

unit_labels = {1: "kg", 2: "number"}
maj_disp_labels = {
    1: "local market (incl. local traders)",
    2: "APMC market",
    3: "input dealers",
    4: "cooperative",
    5: "Government agencies",
    6: "Farmer producer organisations (FPO)",
    7: "private processors",
    8: "contract farming sponsors/ companies",
    9: "others",
}
satisfactory_labels = {
    1: "satisfactory",
    2: "not satisfactory: lower than market price",
    3: "delayed payments",
    4: "deductions for loans borrowed",
    5: "faulty weighing and grading",
    9: "other cause of dissatisfaction",
}


# df['crop_code'] = df['crop_code'].map(crop_code_labels)
df["unit_code"] = df["unit_code"].map(unit_labels)
df["major_disp_sold"] = df["major_disp_sold"].map(maj_disp_labels)
df["satisfied_sale_outcome"] = df["satisfied_sale_outcome"].map(satisfactory_labels)

csv_path = Path.joinpath(interim_data_path, "level6.csv")
df.to_csv(csv_path, index=False)

"""********************************************"""

"""**************LEVEL 7**********************"""
df = pd.read_stata(Path.joinpath(raw_data_path, "level7.dta"))

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
        "Blank",
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
    print("Common ID is unique for Level 7")
else:
    print("Common ID not unique for Level 7")


df["crop_code"] = df["crop_code"].map(crop_code_labels)

csv_path = Path.joinpath(interim_data_path, "level7.csv")
df.to_csv(csv_path, index=False)


"""********************************************"""

"""**************LEVEL 8**********************"""
df = pd.read_stata(Path.joinpath(raw_data_path, "level8.dta"))

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
        "Blank",
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
    print("Common ID is unique for Level 8")
else:
    print("Common ID not unique for Level 8")


where_procure_labels = {
    1: "local market (incl. local traders)",
    2: "APMC market",
    3: "input dealers",
    4: "cooperative",
    5: "Government agencies",
    6: "Farmer producer organisations (FPO)",
    7: "private processors",
    8: "contract farming sponsors/ companies",
    10: "own farm",
    9: "others",
}
quality_labels = {1: "good", 2: "satisfactory", 3: "poor", 4: "dont know"}

df["input"] = df["sl_no"]
input_labels = {
    1: "seeds",
    2: "seeds",
    3: "seeds",
    4: "seeds",
    5: "seeds",
    6: "chemical fertilizers",
    7: "bio-fertilizers",
    8: "manures",
    9: "plant protection materials chemical",
    10: "plant protection materials bio-pesticides",
    11: "diesel",
    12: "electricity",
    13: "irrigation",
    14: "labour human",
    15: "labour animal",
    16: "minor repair and maintenance of machinery and equipment used in crop production",
    17: "interest on loans utilised for the purpose of crop production",
    18: "cost of hiring of machinery and equipment for crop production",
    19: "cost of crop insurance",
    20: "lease rent for land used for crop production",
    21: "other expenses for crop production",
    22: "total",
}

df["input"] = df["input"].map(input_labels)
df["inputs_from_where_procur"] = df["inputs_from_where_procur"].map(
    where_procure_labels
)
df["inputs_qual_adeq_code"] = df["inputs_qual_adeq_code"].map(quality_labels)
df["crop_code"] = df["crop_code"].map(crop_code_labels)

csv_path = Path.joinpath(interim_data_path, "level8.csv")
df.to_csv(csv_path, index=False)

"""********************************************"""


"""**************LEVEL 9**********************"""

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


"""********************************************"""


"""**************LEVEL 10**********************"""

df = pd.read_stata(Path.joinpath(raw_data_path, "level10.dta"))

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
    print("Common ID is unique for Level 10")
else:
    print("Common ID not unique for Level 10")

df["animal_farming_category"] = df["sl_no"]

animal_labels = {
    1: "milk (cattle) (litre)",
    2: "milk ( buffalo) (litre)",
    3: "milk (sheep goat, etc.) (litre)",
    4: "egg (poultry, duck, etc.) (no.)",
    5: "wool (sheep, etc.) (kg)",
    6: "fish (kg)",
    7: "livestock cattle (nos.)",
    8: "livestock buffalo (nos.)",
    9: " livestock sheep, goat, etc. (nos.)",
    10: "livestock pig (nos.)",
    11: "livestock poultry, duck, etc. (nos.)",
    12: "other livestock (nos.)",
    13: "skin, hide, bones",
    14: "manure",
    15: "value of other produce (Rs.)",
    16: "total value of produce",
}

df["animal_farming_category"] = df["animal_farming_category"].map(animal_labels)

csv_path = Path.joinpath(interim_data_path, "level10.csv")
df.to_csv(csv_path, index=False)

"""********************************************"""


"""**************LEVEL 11**********************"""

df = pd.read_stata(Path.joinpath(raw_data_path, "level11.dta"))

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
    print("Common ID is unique for Level 11")
else:
    print("Common ID not unique for Level 11")

df["animal_farming_category"] = df["sl_no"]

animal_labels = {
    1: "milk (cattle) (litre)",
    2: "milk ( buffalo) (litre)",
    3: "milk (sheep goat, etc.) (litre)",
    4: "egg (poultry, duck, etc.) (no.)",
    5: "wool (sheep, etc.) (kg)",
    6: "fish (kg)",
    7: "livestock cattle (nos.)",
    8: "livestock buffalo (nos.)",
    9: " livestock sheep, goat, etc. (nos.)",
    10: "livestock pig (nos.)",
    11: "livestock poultry, duck, etc. (nos.)",
    12: "other livestock (nos.)",
    13: "skin, hide, bones",
    14: "manure",
    15: "value of other produce (Rs.)",
    16: "total value of produce",
}

df["animal_farming_category"] = df["animal_farming_category"].map(animal_labels)

csv_path = Path.joinpath(interim_data_path, "level11.csv")
df.to_csv(csv_path, index=False)

"""********************************************"""


"""**************LEVEL 12**********************"""

df = pd.read_stata(Path.joinpath(raw_data_path, "level12.dta"))

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
    print("Common ID is unique for Level 12")
else:
    print("Common ID not unique for Level 12")

from_where_procured_labels = {
    1: "local market (incl. local traders)",
    2: "APMC market",
    3: "input dealers",
    4: "cooperative",
    5: "Government agencies",
    6: "Farmer producer organisations (FPO)",
    7: "private processors",
    8: "contract farming sponsors/companies",
    9: "others",
    10: "own farm",
}
df["place_procured_code"] = df["place_procured_code"].map(from_where_procured_labels)

df["input_item"] = df["sl_no"]

input_item_labels = {
    1: "animal 'seeds' - cattle",
    2: "animal 'seeds' - buffalo",
    3: "animal 'seeds' - sheep, goat, etc.",
    4: "animal 'seeds' - piggery",
    5: "animal 'seeds' - poultry & duckery",
    6: "animal 'seeds' - others (incl. fishery)",
    7: "animal feed - green fodder",
    8: "animal feed - dry fodder",
    9: "animal feed - concentrates",
    10: "animal feed - others",
    11: "veterinary services - for breeding",
    12: "veterinary services - health services",
    13: "Interest for loans used for farming of animals",
    14: "lease rent for land used for farming of animals",
    15: "labour charges",
    16: "cost of livestock insurance (apportioned for the reference month, if paid annually)",
    17: "other expenses",
    18: "total expenses (1 to 17)",
}

df["input_item"] = df["input_item"].map(input_item_labels)
input_col = df.pop("input_item")
df.insert(5, "input_item", input_col)
quality_variables = {1: "good", 2: "satisfactory", 3: "poor", 4: "don't know"}
df["quality"] = df["quality"].map(quality_variables)


csv_path = Path.joinpath(interim_data_path, "level12.csv")
df.to_csv(csv_path, index=False)

"""********************************************"""


"""**************LEVEL 13**********************"""
df = pd.read_stata(Path.joinpath(raw_data_path, "level13.dta"))


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
    print("Common ID is unique for Level 13")
else:
    print("Common ID not unique for Level 13")

csv_path = Path.joinpath(interim_data_path, "level13.csv")
df.to_csv(csv_path, index=False)

"""********************************************"""


"""**************LEVEL 14**********************"""
df = pd.read_stata(Path.joinpath(raw_data_path, "level14.dta"))

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

df["item"] = df["sl_no"]

item_labels = {
    1: "for farm business: land",
    2: "for farm business: building for farm business",
    3: "for farm business: fish tank",
    4: "for farm business: livestock (cattle, buffalo, sheep, goats etc.)",
    5: "for farm business: poultry/duckery etc.",
    6: "for farm business: sickle, chaff-cutter, axe, spade, chopper, plough, harrow etc.",
    7: "for farm business: power tiller, tractor, combine harvester etc.",
    8: "for farm business: thresher, cane crusher, oil crusher etc.",
    9: "for farm business: pump and other water lifting equipment",
    10: "for farm business: others",
    11: "for non-farm business: land and building for non-farm business",
    12: "for non-farm business: machinery and equipment",
    13: "for non-farm business: others",
    14: "residential buildings including land",
    15: "total (1 to 14)",
}

df["item"] = df["item"].map(item_labels)

item_col = df.pop("item")

df.insert(5, "item", item_col)


if df["common_id"].is_unique:
    print("Common ID is unique for Level 14")
else:
    print("Common ID not unique for Level 14")


csv_path = Path.joinpath(interim_data_path, "level14.csv")
df.to_csv(csv_path, index=False)

"""********************************************"""


"""**************LEVEL 15**********************"""
df = pd.read_stata(Path.joinpath(raw_data_path, "level15.dta"))


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
    print("Common ID is unique for Level 15")
else:
    print("Common ID not unique for Level 15")


nature_of_loan_labels = {
    1: "hereditary loan",
    2: "loan contracted in cash",
    3: "loan contracted in kind",
    4: "loan contracted partly in cash and partly in kind",
}

source_labels = {
    1: "scheduled commercial bank",
    2: "regional rural bank",
    3: "co-operative society",
    4: "co-operative bank",
    5: "insurance companies",
    6: "provident fund",
    7: "employer",
    8: "financial corporation/institution",
    9: "other",
    10: "NBFCs including micro-financing institution",
    11: "bank linked SHG/JLG",
    12: "non-bank linked SHG/JLG",
    13: "other institutional agencies",
    14: "landlord",
    15: "agricultural moneylender",
    16: "professional moneylender",
    17: "input supplier",
    18: "relatives and friends",
    19: "Chit fund",
    20: "Market commission agent/traders",
}

purpose_labels = {
    1: "capital expenditure in farm business",
    2: "revenue expenditure in farm business",
    3: "non-farm business",
    4: "for housing",
    5: "marriages and ceremonies",
    6: "education",
    7: "medical",
    8: "other consumption expenditure",
    9: "other",
}

tenure_of_loan_labels = {
    1: "tenure of loan: short term (less than 1 year)",
    2: "medium term (1 to 3 years)",
    3: "long-term (3 years or more)",
}

df["nature_of_loan"] = df["nature_of_loan"].map(nature_of_loan_labels)
df["source"] = df["source"].map(source_labels)
df["purpose"] = df["purpose"].map(purpose_labels)
df["tenure_of_loan"] = df["tenure_of_loan"].map(tenure_of_loan_labels)

csv_path = Path.joinpath(interim_data_path, "level15.csv")
df.to_csv(csv_path, index=False)

"""********************************************"""


"""**************LEVEL 16**********************"""
df = pd.read_stata(Path.joinpath(raw_data_path, "level16.dta"))

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
    print("Common ID is unique for Level 16")
else:
    print("Common ID not unique for Level 16")

aware_labels = {1: "yes", 2: "no"}

unit_code_labels = {1: "kg", 2: "number"}

agency_procures_crop_at_msp_labels = {
    1: "yes: FCI",
    2: "yes: JCI",
    3: "yes: CCI",
    4: "yes: NAFED",
    5: "yes: State Food Corporation",
    6: "yes: State Civil Supplies",
    7: "yes: Others",
    9: "do not know",
}

sell_to_agencies_labels = {
    1: "yes, sold to: FCI",
    2: "yes, sold to: JCI",
    3: "yes, sold to: CCI",
    4: "yes, sold to: NAFED",
    5: "yes, sold to: State Food Corporation",
    6: "yes, sold to: State Civil Supplies",
    7: "yes, sold to: Others",
    9: "do not know",
}

reason_labels = {
    1: "procurement agency not available",
    2: "no local purchaser",
    3: "poor quality of crop",
    4: "crop already pre-pledged",
    5: "received better price over MSP",
    9: "others",
}

crop_code_labels_1 = {
    101: "paddy",
    203: "urad",
    1007: "sunflower seed",
    102: "jowar",
    204: "moong",
    1008: "safflower",
    103: "bajra",
    205: "masur (lentil)",
    1010: "nigerseed",
    1009: "soyabean",
    104: "maize",
    401: "sugarcane",
    105: "ragi",
    1001: "groundnut in shell",
    1012: "toria",
    106: "wheat",
    1003: "sesamum",
    1101: "cotton",
    107: "barley",
    1004: "rapseed/mustard",
    1102: "jute",
    201: "gram",
    1006: "copra",
    202: "arhar(tur)",
}
df["aware_of_msp"] = df["aware_of_msp"].map(aware_labels)
df["unit_code"] = df["unit_code"].map(unit_code_labels)
df["crop_code"] = df["crop_code"].map(crop_code_labels_1)
df["agency_procures_crop_at_msp"] = df["agency_procures_crop_at_msp"].map(
    agency_procures_crop_at_msp_labels
)
df["sell_to_agencies"] = df["sell_to_agencies"].map(sell_to_agencies_labels)
df["reason_code"] = df["reason_code"].map(reason_labels)

csv_path = Path.joinpath(interim_data_path, "level16.csv")
df.to_csv(csv_path, index=False)

"""********************************************"""


"""**************LEVEL 17**********************"""

df = pd.read_stata(Path.joinpath(raw_data_path, "level17.dta"))

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
    print("Common ID is unique for Level 17")
else:
    print("Common ID not unique for Level 17")

df["source"] = df["sl_no"]
source_labels = {
    1: "progressive farmer",
    2: "input dealers",
    3: "Government extension agent/ATMA",
    4: "Krishi Vigyan Kendra",
    5: "agricultural university /college",
    6: "private commercial agents (including contract farming sponsors/ companies, drilling contractors etc.)",
    7: "veterinary department",
    8: "cooperatives/ Dairy cooperatives",
    9: "Farmer Producer organisations (FPOs)",
    10: "private processors",
    11: "Agri. Clinics& Agri. Business Centres (ACABC)",
    12: "NGO",
    13: "Kisan Call Centre",
    14: "print media",
    15: "radio/TV/ other electronic media",
    16: "smart phone apps based information",
}

df["source"] = df["source"].map(source_labels)
source_col = df.pop("source")
df.insert(5, "source_of_technical_advice", source_col)


whether_accessed_labels = {1: "yes", 2: "no"}

whether_recommended_advice_adopted_labels = {1: "yes", 2: "no"}

type_of_information_accessed_labels = {
    11: "cultivation: improved seed/variety",
    12: "cultivation: fertilizer application",
    13: "cultivation: plant protection(pesticide etc.)",
    14: "cultivation: farm machinery",
    15: "cultivation: harvesting/marketing",
    19: "cultivation: others",
    21: "animal husbandry : breeding",
    22: "animal husbandry : feeding",
    23: "animal husbandry : health care",
    24: "animal husbandry : management",
    29: "animal husbandry : others",
    31: "fishery: seed production",
    32: "fishery: harvesting",
    33: "fishery: management and marketing",
    39: "fishery: others",
}

df["whether_accessed"] = df["whether_accessed"].map(whether_accessed_labels)
df["whet_reco_advice_adopted"] = df["whet_reco_advice_adopted"].map(
    whether_recommended_advice_adopted_labels
)
df["type_information_accessed"] = df["type_information_accessed"].map(
    type_of_information_accessed_labels
)


csv_path = Path.joinpath(interim_data_path, "level17.csv")
df.to_csv(csv_path, index=False)

"""********************************************"""


"""**************LEVEL 18**********************"""
df = pd.read_stata(Path.joinpath(raw_data_path, "level18.dta"))

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
    print("Common ID is unique for Level 18")
else:
    print("Common ID not unique for Level 18")


whet_crop_insured_labels = {
    1: "insured only when received loan",
    2: "insured additionally",
    3: "not insured",
}

reason_for_not_insuring_labels = {
    1: "not aware",
    2: "not aware about availability of facility",
    3: "not interested",
    4: "no need",
    5: "insurance facility not available",
    6: "lack of resources for premium payment",
    7: "not satisfied with terms & conditions",
    8: "nearest bank at a long distance",
    9: "others",
    10: "complex procedures",
    11: "delay in claim payment",
}

rcpt_insurance_docu_cert_labels = {1: "yes", 2: "no"}

any_crop_loss_labels = {1: "yes", 2: "no"}

cause_of_crop_loss_labels = {
    1: "inadequate rainfall/drought",
    2: "disease/insect/animal",
    3: "flood",
    4: "other natural causes (fire, lighting, storm, cyclone, earthquake etc.)",
    9: "others",
}


received_claim_labels = {1: "yes: fully", 2: "yes: partly", 3: "no"}

time_taken_receive_claim_labels = {
    1: "within 6 months",
    2: "6 to 12 months",
    3: "more than 12 months",
}

reason_not_receiving_claim_labels = {
    1: "cause outside coverage",
    2: "documents lost",
    9: "others",
}


df["crop_code"] = df["crop_code"].map(crop_code_labels)
df["whet_crop_insured"] = df["whet_crop_insured"].map(whet_crop_insured_labels)
df["reason_for_not_insuring"] = df["reason_for_not_insuring"].map(
    reason_for_not_insuring_labels
)
df["rcpt_insurance_docu_cert"] = df["rcpt_insurance_docu_cert"].map(
    rcpt_insurance_docu_cert_labels
)
df["any_crop_loss"] = df["any_crop_loss"].map(any_crop_loss_labels)
df["cause_of_crop_loss"] = df["cause_of_crop_loss"].map(cause_of_crop_loss_labels)
df["received_claim"] = df["received_claim"].map(received_claim_labels)
df["time_taken_receive_claim"] = df["time_taken_receive_claim"].map(
    time_taken_receive_claim_labels
)
df["reason_not_receiving_claim"] = df["reason_not_receiving_claim"].map(
    reason_not_receiving_claim_labels
)

csv_path = Path.joinpath(interim_data_path, "level18.csv")
df.to_csv(csv_path, index=False)

"""*********************End*********************"""
