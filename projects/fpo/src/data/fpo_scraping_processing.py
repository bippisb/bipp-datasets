import pandas as pd
from tabula.io import read_pdf

# Reading from Non-SFAC FPOs PDF into list of dataframes

df = read_pdf(r"/content/Statewise list of FPOs.cleaned.pdf", lattice=True, pages="all")

# Concatenating each dataframe in the list to create final FPO dataframe

fpo = pd.DataFrame(df[1])

for i in range(2, len(df)):
    fpo = pd.concat([fpo, df[i]], axis=0, join="outer")

# Cleaning Non SFAC FPO data

fpo.drop([0], axis=0, inplace=True)

fpo.drop(
    ["Statewise list of Farmer Producer Orgranisations (FPOs)"], axis=1, inplace=True
)

fpo1 = fpo.rename(
    columns={
        "Check Point Threat Extraction secured this document": "s_no",
        "Unnamed: 0": "state",
        "Unnamed: 1": "fpo_name",
        "Unnamed: 2": "legal_form",
        "Unnamed: 3": "reg_no",
        "Unnamed: 4": "reg_date",
        "Unnamed: 5": "address",
        "Unnamed: 6": "contact_details",
        "Unnamed: 7": "major_crop_names",
    }
)

fpo2 = fpo1[fpo1["state"] != "State"]

for col in fpo2:
    fpo2[col] = fpo2[col].str.rstrip("\r")
    fpo2[col] = fpo2[col].replace({"\r": " "}, regex=True)

# Saving to CSV

fpo2.to_csv("/content/Non SFAC FPOs_final.csv", index=False)

# Reading SFAC FPO PDF into list of dataframes

df2 = read_pdf(
    r"/content/State wise list of FPOs registered under 2 and 3 year programme.cleaned.pdf",
    lattice=True,
    pages="all",
)

# Concatenating each dataframe in the list to create final FPO dataframe

sfac = pd.DataFrame(df2[0])
for i in range(1, len(df2)):
    sfac = pd.concat([sfac, df2[i]], axis=0)

# Cleaning SFAC FPO dataframe

sfac1 = sfac.drop([0], axis=0)

sfac1.drop(
    [
        "Check Point Threat Extraction secured this documeSntattewise list of FPOs registered under 2 and 3 year programme promoted by SFAC"
    ],
    axis=1,
    inplace=True,
)

sfac1 = sfac1.rename(
    columns={
        "Unnamed: 0": "state",
        "Unnamed: 1": "district",
        "Unnamed: 2": "programme",
        "Unnamed: 3": "resource_institution",
        "Unnamed: 4": "fpo_name",
        "Unnamed: 5": "legal_form",
        "Unnamed: 6": "reg_no",
        "Unnamed: 7": "reg_date",
        "Unnamed: 8": "address",
        "Unnamed: 9": "contact",
        "Unnamed: 10": "major_crops",
    }
)

sfac2 = sfac1.drop(
    ["Statewise list of FPOs registered under 2 and 3 year programme promoted by SFAC"],
    axis=1,
)

for col in sfac2:
    sfac2[col] = sfac2[col].str.rstrip("\r")
    sfac2[col] = sfac2[col].replace({"\r": " "}, regex=True)

# Saving SFAC FPOs dataframe to CSV file

sfac2.to_csv("/content/SFAC FPOs_final.csv", index=False)

# Accessing Districts in Non SFAC FPOs

data = pd.read_excel("/content/Non SFAC FPOs.xlsx")

district = pd.read_csv("/content/lgd_mapper_state_dist_block_gp_village - Copy.csv")

district = district[["state_name", "district_name"]]

dist = district.drop_duplicates()

data["district"] = None

dist = dist.dropna()

data1 = data.dropna(subset=["address"])

for i in data1.index:
    add = data1["address"][i].lower()
    for j in dist.index:
        if type(dist["district_name"][j]) == str:
            if (dist["district_name"][j].lower()) in add:
                data1["district"][i] = dist["district_name"][j]

for i in data1.index:
    if data1["district"][i] is not None:
        if len(data1["district"][i].split(" ")) == 1:
            add = data1["address"][i].upper()
            add_list = add.split(" ")
            for j in range(0, len(add_list) - 2):
                if "DIST" in add_list[j]:
                    if add_list[j + 1] == ":":
                        data1["district"][i] = add_list[j + 2]
                    elif (
                        (add_list[j] == "DIST:")
                        or (add_list[j] == "DIST-")
                        or (add_list[j] == "DISTRICT-")
                    ):
                        data1["district"][i] = add_list[j + 1]
                    elif "-" in add_list[j]:
                        l1 = add_list[j].split("-")
                        data1["district"][i] = l1[-1]
    else:
        add = data1["address"][i].upper()
        add_list = add.split(" ")
        for j in range(0, len(add_list) - 2):
            if "DIST" in add_list[j]:
                if add_list[j + 1] == ":":
                    data1["district"][i] = add_list[j + 2]
                elif (
                    (add_list[j] == "DIST:")
                    or (add_list[j] == "DIST-")
                    or (add_list[j] == "DISTRICT-")
                ):
                    data1["district"][i] = add_list[j + 1]
                elif "-" in add_list[j]:
                    l1 = add_list[j].split("-")
                    data1["district"][i] = l1[-1]

data1["district"].count()

data1.to_csv("Non_SFAC_FPO_district.csv")

dist.to_csv("District.csv")

data2 = pd.read_csv("/content/Non_SFAC_FPO_district_raw.csv")

for i in data2["district"].index:
    if type(data2["district"][i]) == str:
        data2["district"][i] = data2["district"][i].rstrip(",")

data2.to_csv("non_sfac_fpos_final.csv")
