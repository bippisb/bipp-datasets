from pathlib import Path

import pandas as pd

# defining paths
dir_path = Path.cwd()
raw_data_path = Path.joinpath(dir_path, "data", "raw")
interim_data_path = Path.joinpath(dir_path, "data", "interim")


### LEVEL 1###
df = pd.read_stata(Path.joinpath(raw_data_path, "level1.dta"))
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

df1 = df[["common_id", "sector", "district", "multiplier", "w"]]
if df1["common_id"].is_unique:
    print("Common ID is unique for Level 1")
else:
    print("Common ID not unique for Level 1")

csv_path = Path.joinpath(interim_data_path, "level1.csv")
df1.to_csv(csv_path, index=False)

### LEVEL 2 ###

df = pd.read_stata(Path.joinpath(raw_data_path, "level2.dta"))

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
df["deposit_acc_in_cb_rrb_coop"] = df["deposit_acc_in_cb_rrb_coop"].map(bank_labels)

csv_path = Path.joinpath(interim_data_path, "level2.csv")
df.to_csv(csv_path, index=False)

### LEVEL 3 ###

df = pd.read_stata(Path.joinpath(raw_data_path, "level3.dta"))

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
df["land_for_agri_last_365days"] = df["land_for_agri_last_365days"].map(yn_labels)

for i in range(0, len(df["sector"])):
    if df["sector"][i] == 1:
        df["hh_type"][i] = household_rural_labels[df["hh_type"][i]]
    elif df["sector"][i] == 2:
        df["hh_type"][i] = household_urban_labels[df["hh_type"][i]]

csv_path = Path.joinpath(interim_data_path, "level3.csv")
df.to_csv(csv_path, index=False)


### LEVEL 4 ###

df = pd.read_stata(Path.joinpath(raw_data_path, "level4.dta"))
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
    print("Common ID is unique for Level 4")
else:
    print("Common ID not unique for Level 4")

csv_path = Path.joinpath(interim_data_path, "level4.csv")
df.to_csv(csv_path, index=False)


### LEVEL 5 ###
df = pd.read_stata(Path.joinpath(raw_data_path, "level5.dta"))
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
    print("Common ID is unique for Level 5")
else:
    print("Common ID not unique for Level 5")

yn_labels = {1: "Yes", 2: "No"}

land_labels = {
    1: "crop area, irrigated/unirrigated",
    2: "other area for agricultural/farm business",
    3: "for non-farm business",
    10: "residential area including homestead",
    9: "other areas",
}

id_labels = {
    97: "total urban land outside the FSU",
    98: "total homestead land owned 10",
    99: "total land owned",
}

df["type_of_land_code"] = df["type_of_land_code"].map(land_labels)
df["female_members_share"] = df["female_members_share"].map(yn_labels)

csv_path = Path.joinpath(interim_data_path, "level5.csv")
df.to_csv(csv_path, index=False)

### LEVEL 6 ###


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
df["type_of_land_code"] = df["type_of_land_code"].map(type_of_land_labels)
df["female_members_share"] = df["female_members_share"].map(female_members_share_labels)

csv_path = Path.joinpath(interim_data_path, "level6.csv")
df.to_csv(csv_path, index=False)

### LEVEL 7 ###

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

### LEVEL 8 ###

df = pd.read_stata(Path.joinpath(raw_data_path, "level8.dta"))

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
    print("Common ID is unique for Level 8")
else:
    print("Common ID not unique for Level 8")


df.rename(columns={"serial_no": "sl_no."}, inplace=True)
sl_no = df.pop("sl_no.")
df.insert(3, "sl_no.", sl_no)

df["item"] = df["sl_no."]

item_labels = {
    1: "cattle: exotic/ cross-bred/ descript/ nondescript: (a) young stock: (i) young stock (male)",
    2: "cattle: exotic/ cross-bred/ descript/ nondescript: (a) young stock: (ii) young stock (female)",
    3: "cattle: exotic/ cross-bred/ descript/ nondescript: (b) female: (i) breeding cow (milching)",
    4: "cattle: exotic/ cross-bred/ descript/ nondescript: (b) female: (ii) breeding cow: dry/not calved even once",
    5: "cattle: exotic/ cross-bred/ descript/ nondescript: (b) female: (iii) other",
    6: "cattle: exotic/ cross-bred/ descript/ nondescript: (c) male cattle: for work/ breeding/other",
    7: "buffalo: exotic/ cross-bred/ descript/ nondescript: (a) young stock: (i) young stock (male)",
    8: "buffalo: exotic/ cross-bred/ descript/ nondescript: (a) young stock: (ii) young stock (female)",
    9: "buffalo: exotic/ cross-bred/ descript/ nondescript: (b) female: (i) breeding buffalo: in milk",
    10: "buffalo: exotic/ cross-bred/ descript/ nondescript: (b) female: (ii) breeding buffalo: dry/not calved even once",
    11: "buffalo: exotic/ cross-bred/ descript/ nondescript: (b) female: (iii) other",
    12: "buffalo: exotic/ cross-bred/ descript/ nondescript: (c) male: for work/breeding/other",
    13: "sub-total (items 1to 12)",
    14: "ovine and other mammals (sheep, goat, pig, rabbits, etc.)",
    15: "poultry birds (hen, cock, chicken, duck, duckling, other poultry birds, etc.)",
    16: "other including large heads (elephant, camel, horse, mule, pony, donkey, yak, mithun, etc.)",
    17: "total (items 13 to 16)",
}
df["item"] = df["item"].map(item_labels)
item_col = df.pop("item")
df.insert(4, "item_description", item_col)

csv_path = Path.joinpath(interim_data_path, "level8.csv")
df.to_csv(csv_path, index=False)

### LEVEL 9 ###
df = pd.read_stata(Path.joinpath(raw_data_path, "level9.dta"))

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
    print("Common ID is unique for Level 9")
else:
    print("Common ID not unique for Level 9")

df["transport_category"] = df["serial_no"]

transport_labels = {
    1: "tractors (all types)",
    2: "motor cars/jeep/van",
    3: "motorcycles/ scooters/ mopeds/ auto-rickshaws",
    4: "rickshaw/e-rickshaw/toto rickshaw/van rickshaw",
    5: "bicycles",
    6: "carts (hand-driven / animal driven)",
    7: "other transport equipment incl. boats, trucks,trailers, light commercial vehicles (LCV), passenger buses, etc.",
    8: "total",
}

use_labels = {
    1: "for farm business",
    2: "for non-farm business",
    3: "for household use",
}

df["transport_category"] = df["transport_category"].map(transport_labels)
df["equipment_owned"] = df["equipment_owned"].map(use_labels)

csv_path = Path.joinpath(interim_data_path, "level9.csv")
df.to_csv(csv_path, index=False)


### LEVEL 10 ###

df = pd.read_stata(Path.joinpath(raw_data_path, "level10.dta"))

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
    print("Common ID is unique for Level 10")
else:
    print("Common ID not unique for Level 10")

df.rename(columns={"serial_no": "sl_no."}, inplace=True)
sl_no = df.pop("sl_no.")
df.insert(3, "sl_no.", sl_no)

df["item"] = df["sl_no."]

item_labels = {
    1: "power tiller/power driven plough etc.",
    2: "crop harvester (power driven)/combined harvester",
    3: "thresher, other power driven machinery and equipment",
    4: "laser land leveler",
    5: "manually operated implements/tools (inc. sickle, chaffcutter, axe, spade, chopper, plough, harrow etc.)",
    6: "diesel pumps",
    7: "electric pumps",
    8: "drip sprinkler",
    9: "other machineries for irrigation",
    10: "capital work-in-progress(agricultural machinery and equipment under installation)",
    11: "other not covered in items 1 to 10excluding furniture and fixtures",
    12: "furniture and fixtures",
    13: "total (items 1 to 12)",
}

df["item"] = df["item"].map(item_labels)
item_col = df.pop("item")
df.insert(4, "item_description", item_col)

csv_path = Path.joinpath(interim_data_path, "level10.csv")
df.to_csv(csv_path, index=False)

### LEVEL 11 ###

df = pd.read_stata(Path.joinpath(raw_data_path, "level11.dta"))

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
    print("Common ID is unique for Level 11")
else:
    print("Common ID not unique for Level 11")

df.rename(columns={"serial_no": "sl_no."}, inplace=True)
sl_no = df.pop("sl_no.")
df.insert(3, "sl_no.", sl_no)

df["item"] = df["sl_no."]

item_labels = {
    1: "handloom, semi-automatic and power looms, ginning, pressing and baling equipment",
    2: "reeds, bobbins and other items used in spinning and weaving and tailoring equipment, and related accessories",
    3: "equipment used in beauty salon/spa",
    4: "instruments used in gyms",
    5: "equipment for maintaining and repairing cycles/rickshaw/automobile",
    6: "mills (e.g. ghanies, oil-mills/crusher (power-driven), rice-milling including crusher and pounding equipment, flour-milling and grinding equipment), cane crusher etc.",
    7: "electric motors, generators, pump sets, inverters, etc.",
    8: "casting, melting and welding equipment, furnace, bellows, kiln, potter's wheels, cobbler's tools etc.",
    9: "scales, weights and measures",
    10: "saw (all types), carpentry tools, electric drilling machines and other related tools and machines",
    11: "Xerox/ duplicating machine, camera, lamination machine, fax machine, printing press, personal computer, printer, other ICT equipment etc.",
    12: "tools for mobile repairing , computer repairing, etc.",
    13: "X- ray machine, ultra sound machine, ECG machines, other medical equipment",
    14: "lathes, other machinery tools& appliances",
    15: "total: machinery, tools & appliances (items 1 to 14)",
    16: "intellectual property product (intangible assets) like software, database, trademark, manuscripts, copyrights, etc.",
    17: "capital work-in-progress (non-farm business equipment under installation/ software development )",
    18: "other non-farm business equipment not covered in item 1 -14, 16-17, excluding furniture and fixtures",
    19: "furniture fixtures",
    20: "total (item 15 + items 16 to 19)",
}

df["item"] = df["item"].map(item_labels)
item_col = df.pop("item")
df.insert(4, "item_description", item_col)

csv_path = Path.joinpath(interim_data_path, "level11.csv")
df.to_csv(csv_path, index=False)

### LEVEL 12 ###
df = pd.read_stata(Path.joinpath(raw_data_path, "level12.dta"))

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
    print("Common ID is unique for Level 12")
else:
    print("Common ID not unique for Level 12")

df.rename(columns={"serial_no": "sl_no."}, inplace=True)
sl_no = df.pop("sl_no.")
df.insert(3, "sl_no.", sl_no)

df["item"] = df["sl_no."]

item_labels = {
    1: "cash in hand",
    2: "amount in current bank account",
    3: "Deposit: deposit in savings bank account (excl. Post Office Savings Bank POSB)",
    4: "Deposit: fixed deposit/ term deposit/ RD / flexi- RD in banks (excl. POSB)",
    5: "Deposit: savings and/or fixed deposits in post office savings bank",
    6: "Deposit: other fixed income deposits (NSC, KVP, saving bonds, other small savings schemes, etc.)",
    7: "Deposit: deposits in cooperative banks",
    8: "Deposit: deposits with non-banking finance companies",
    9: "Deposit: deposits with Co-op credit society/micro-finance institutions/self-help groups",
    10: "PF/Pension fund: contributions to provident fund (GPF/PPF/EPF etc.)",
    11: "PF/Pension fund: contributions to pension fund& NPS/other contributory funds/annuity schemes",
    12: "Life Insurance: total no. of insurance policies in the name of household member(s)",
    13: "Life Insurance: total sum assured",
    14: "Life Insurance: amount received under money back policies etc.",
    15: "other financial savings (deposits with other enterprises, individuals, chit fund contributions etc.)",
    16: "other receivable: interest free loan given to others including friends and relatives",
    17: "other receivable: business loans given to others",
    18: "other receivable: personal loans given to others",
    19: "total (items 1 to 18)",
    20: "bullion & ornaments (incl. gold jewellery, gems & precious stones etc.)",
    21: "paintings and artistic originals",
}

df["item"] = df["item"].map(item_labels)
item_col = df.pop("item")
df.insert(4, "item_description", item_col)

csv_path = Path.joinpath(interim_data_path, "level12.csv")
df.to_csv(csv_path, index=False)

### LEVEL 13 ###

df = pd.read_stata(Path.joinpath(raw_data_path, "level13.dta"))

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
    print("Common ID is unique for Level 13")
else:
    print("Common ID not unique for Level 13")

df.rename(columns={"serial_no": "sl_no."}, inplace=True)
sl_no = df.pop("sl_no.")
df.insert(3, "sl_no.", sl_no)

df["item"] = df["sl_no."]

item_labels = {
    1: "mutual fund",
    2: "shares in companies",
    3: "debentures/bonds in companies",
    4: "shares in co-operative society",
    5: "total (item 1 to 4)",
}

df["item"] = df["item"].map(item_labels)
item_col = df.pop("item")
df.insert(4, "type of instrument", item_col)

csv_path = Path.joinpath(interim_data_path, "level13.csv")
df.to_csv(csv_path, index=False)

### LEVEL 14 ###

df = pd.read_stata(Path.joinpath(raw_data_path, "level14.dta"))
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
    print("Common ID is unique for Level 14")
else:
    print("Common ID not unique for Level 14")

df.rename(columns={"serial_no": "sl_no."}, inplace=True)
sl_no = df.pop("sl_no.")
df.insert(3, "sl_no.", sl_no)


whether_loan_unpaid_labels = {1: "yes", 2: "no"}

credit_agency_labels = {
    1: "scheduled commercial bank",
    2: "regional rural bank",
    3: "co-operative society",
    4: "co-operative bank",
    5: "insurance companies",
    6: "provident fund",
    7: "employer",
    8: "financial corporation/institution",
    9: "other",
    10: "NBFCs including micro-financing institution (MFIs)",
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

scheme_of_lending_labels = {
    1: "Mudra",
    2: "Stand-Up India scheme",
    3: "NRLM/NULM (National Rural/Urban Livelihood Mission)",
    4: "other central govt schemes",
    5: "exclusive state scheme",
    6: "exclusive bank scheme",
    7: "kisan credit card",
    8: "crop loan/ other agricultural loan",
    9: "not covered under any scheme",
}

tenure_of_loan_labels = {
    1: "short-term (less than 1 year)",
    2: "medium term (1 to 3 year)",
    3: "long-term (3 year or more)",
}

nature_of_interest_labels = {1: "interest free", 2: "simple", 3: "compound"}

purpose_of_loan_labels = {
    1: "capital expenditure in farm business",
    2: "revenue expenditure in farm business",
    3: "capital expenditure in non-farm business",
    4: "revenue expenditure in non-farm business",
    5: "expenditure on litigation",
    6: "repayment of debt",
    7: "financial investment expenditure",
    8: "for education",
    9: "other",
    10: "for medical treatment",
    11: "for housing",
    12: "for other household expenditure",
}

df["whether_loan_unpaid"] = df["whether_loan_unpaid"].map(whether_loan_unpaid_labels)
df["credit_agency"] = df["credit_agency"].map(credit_agency_labels)
df["scheme_of_lending"] = df["scheme_of_lending"].map(scheme_of_lending_labels)
df["tenure_of_loan"] = df["tenure_of_loan"].map(tenure_of_loan_labels)
df["nature_of_interest"] = df["nature_of_interest"].map(nature_of_interest_labels)
df["purpose_of_loan"] = df["purpose_of_loan"].map(purpose_of_loan_labels)


csv_path = Path.joinpath(interim_data_path, "level14.csv")
df.to_csv(csv_path, index=False)

### LEVEL 15 ###

df = pd.read_stata(Path.joinpath(raw_data_path, "level15.dta"))

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
    print("Common ID is unique for Level 15")
else:
    print("Common ID not unique for Level 15")


df.rename(columns={"serial_no": "sl_no."}, inplace=True)
sl_no = df.pop("sl_no.")
df.insert(3, "sl_no.", sl_no)


period_of_kind_loan_labels = {
    1: "less than 1 month",
    2: "1 month and above but less than 3 months",
    3: "3 months and above but less than 6 months",
    4: "6 months & above but less than 1 year",
    5: "one year & above",
}

source_of_kind_loan_labels = {
    1: "input supplier",
    2: "relatives & friends",
    3: "doctor, lawyers and other professionals",
    9: "Other",
}

purpose_labels = {
    1: "revenue expenditure in farm business",
    2: "revenue expenditure in non-farm business",
    3: "household expenditure",
    9: "other expenditure",
}


df["period_of_kind_loan"] = df["period_of_kind_loan"].map(period_of_kind_loan_labels)
df["source_of_kind_loan"] = df["source_of_kind_loan"].map(source_of_kind_loan_labels)
df["purpose"] = df["purpose"].map(purpose_labels)

csv_path = Path.joinpath(interim_data_path, "level15.csv")
df.to_csv(csv_path, index=False)

### LEVEL 16 ###

df = pd.read_stata(Path.joinpath(raw_data_path, "level16.dta"))

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
    print("Common ID is unique for Level 16")
else:
    print("Common ID not unique for Level 16")


df.rename(columns={"serial_no": "sl_no."}, inplace=True)
sl_no = df.pop("sl_no.")
df.insert(3, "sl_no.", sl_no)

df["item"] = df["sl_no."]

item_labels = {
    1: "land",
    2: "houses, buildings and other constructions (including farmhouses) : purchase",
    3: "houses, buildings and other constructions (including farmhouses) : addition",
    4: "farm business: land",
    5: "farm business: land rights",
    6: "farm business: building, barns& animals sheds",
    7: "farm business: orchard & plantations",
    8: "farm business: fish tank (all type)",
    9: "farm business: wells, bore-wells, tubewells,field distribution systems, other construction and irrigation resources",
    10: "farm business: pump and other water lifting equipment",
    11: "farm business: sickle, chaff-cutter, axe, spade, chopper, plough, harrow etc.",
    12: "farm business: power tiller, thresher, cane crusher, oil crusher,combined harvester,etc.",
    13: "farm business: livestock: working/breeding cattle& buffaloes",
    14: "farm business: livestock: egg-laying ducks and hens",
    15: "farm business: transport equipment incl. tractor used for farm business only",
    16: "farm business: other",
    97: "farm business: sub-total (items 1 to 16)",
    17: "non-farm business: land",
    18: "non-farm business: workplace, workshop/manufacturing unit, shop & other constructions",
    19: "non-farm business: non-farm business equipment & accessories",
    20: "non-farm business: transport equipment incl. tractor used for non-farm business only",
    21: "non-farm business: other",
    98: "non-farm business: sub-total (items 17 to 21)",
}

df["item"] = df["item"].map(item_labels)
item_col = df.pop("item")
df.insert(4, "item_description", item_col)

csv_path = Path.joinpath(interim_data_path, "level16.csv")
df.to_csv(csv_path, index=False)

### LEVEL 17 ###

df = pd.read_stata(Path.joinpath(raw_data_path, "level17.dta"))
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
    print("Common ID is unique for Level 17")
else:
    print("Common ID not unique for Level 17")


df.rename(columns={"serial_no": "sl_no."}, inplace=True)
sl_no = df.pop("sl_no.")
df.insert(3, "sl_no.", sl_no)

df["item"] = df["sl_no."]

item_labels = {
    1: "land",
    2: "houses, buildings and other constructions (including farmhouses) : purchase",
    3: "houses, buildings and other constructions (including farmhouses) : addition",
    4: "farm business: land",
    5: "farm business: land rights",
    6: "farm business: building, barns& animals sheds",
    7: "farm business: orchard & plantations",
    8: "farm business: fish tank (all type)",
    9: "farm business: wells, bore-wells, tubewells,field distribution systems, other construction and irrigation resources",
    10: "farm business: pump and other water lifting equipment",
    11: "farm business: sickle, chaff-cutter, axe, spade, chopper, plough, harrow etc.",
    12: "farm business: power tiller, thresher, cane crusher, oil crusher,combined harvester,etc.",
    13: "farm business: livestock: working/breeding cattle& buffaloes",
    14: "farm business: livestock: egg-laying ducks and hens",
    15: "farm business: transport equipment incl. tractor used for farm business only",
    16: "farm business: other",
    97: "farm business: sub-total (items 1 to 16)",
    17: "non-farm business: land",
    18: "non-farm business: workplace, workshop/manufacturing unit, shop & other constructions",
    19: "non-farm business: non-farm business equipment & accessories",
    20: "non-farm business: transport equipment incl. tractor used for non-farm business only",
    21: "non-farm business: other",
    98: "non-farm business: sub-total (items 17 to 21)",
}

df["item"] = df["item"].map(item_labels)
item_col = df.pop("item")
df.insert(4, "item_description", item_col)

csv_path = Path.joinpath(interim_data_path, "level17.csv")
df.to_csv(csv_path, index=False)
