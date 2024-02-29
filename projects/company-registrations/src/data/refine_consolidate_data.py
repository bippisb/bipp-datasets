# %%
import glob
import os

import numpy as np
import pandas as pd
from dateutil import parser

# Folder paths
dir_path = os.getcwd()
data_path = os.path.join(dir_path, "data")
raw_path = os.path.join(data_path, "raw")
interim_path = os.path.join(data_path, "interim")

code_mapp = pd.read_csv("./data/external/mca_codes.csv")
unique_code_mapp = code_mapp.drop_duplicates(subset="act_code", keep="first")
# create dict from unique_code_mapp act_code as key and act_description as value
unique_code_mapp = unique_code_mapp.set_index("act_code")[
    "act_description"
].to_dict()
lgd_codes = pd.read_csv("./data/external/lgd_codes.csv")
lgd_codes = lgd_codes[["state_name", "state_code"]].drop_duplicates(
    subset="state_name", keep="first"
)


def parse_date(date_str):
    # List of date formats to try
    date_formats = [
        "%d-%m-%Y",  # 01-01-2016
        "%d/%m/%Y",  # 19/04/2016
        "%d.%m.%Y",  # 24.08.2016
        "%Y-%m-%d %H:%M:%S",  # 2016-12-20 00:00:00
        "%Y-%m-%d",  # 2017-08-29
    ]
    # Try each format
    for fmt in date_formats:
        try:
            return pd.to_datetime(date_str, format=fmt).strftime("%d-%m-%Y")
        except ValueError:
            continue
    # Fallback to dateutil parser if no format matches
    try:
        return parser.parse(date_str).strftime("%d-%m-%Y")
    except ValueError:
        # Handle the case where the date_str format is unknown or invalid
        return None


# %%
col_rename = {
    "industrial description": "act_description",
    "date_of_registration": "registration_date",
    "activity_description": "act_description",
    "activity_code": "act_code",
    "paidup": "paidup_capital",
    "roc location": "roc",
    "nature of company": "company_nature",
    "activity": "act_code",
    "company sub-category": "company_sub_category",
    "company class": "company_class",
    "address": "company_address",
    "paid up": "paidup_capital",
    "activity description": "act_description",
    "state": "state_name",
    "sub category": "company_sub_category",
    "paidup_capital": "paidup_capital",
    "paidup capital": "paidup_capital",
    "cin": "cin",
    "company category": "company_category",
    "authorized capital": "authorized_capital",
    "company": "company_status",
    "company name": "company_name",
    "date of registration": "registration_date",
    "paid capital": "paidup_capital",
    "company_name": "company_name",
    "authorized": "authorized_capital",
    "category": "company_category",
    "month": "month",
    "authorized capital (in thousand rs.)": "authorized_capital",
    "paid-up capital (in thousand rs.)": "paidup_capital",
    "company email id": "company_email",
    "company type": "company_type",
    "month_name": "month",
    "month name": "month",
    "company_type": "company_type",
    "activity code": "act_code",
    "listed flag": "listed_flag",
    "registered_office_address": "company_address",
    "industrial activity code": "act_code",
    "date_string": "date_string",
    "registered office address": "company_address",
    "company_status": "company_status",
    "company address": "company_address",
    "inc-29": "inc_29",
    "authorized_capital": "authorized_capital",
    "description": "act_description_1",
    "class": "company_class",
    "company status": "company_status",
    "number of members (applicable only in case of co. without share capital)": "members",
    "date of incorporation": "registration_date",
    "email": "company_email",
    "industrial activity": "act_code",
}
desc_rename = {
    "Manufacturing (Paper & Paper products, Publishing, printing": "Manufacturing (Paper & Paper products, Publishing, printing and reproduction of recorded media)",
    "nan": None,
    np.nan: None,
    "": None,
    "Manufacturing (Paper & Paper products, Publishing, printingand reproduction of recorded media)": "Manufacturing (Paper & Paper products, Publishing, printing and reproduction of recorded media)",
    "Building of complete constructions or parts thereof; civil e": "Construction",
    "Trading ": "Trading",
    "Agriculture and Allied Activities ": "Agriculture and Allied Activities",
    "Community, personal & Social Services ": "Community, personal & Social Services",
    "Manufacturing (Metals & Chemicals, and products thereof) ": "Manufacturing (Metals & Chemicals, and products thereof)",
    "Finance ": "Finance",
    "Business Services ": "Business Services",
    "Manufacturing (Textiles) ": "Manufacturing (Textiles)",
    "Transport, storage and Communications ": "Transport, storage and Communications",
    "Real Estate and Renting ": "Real Estate and Renting",
    "Construction ": "Construction",
    "Manufacturing (Others) ": "Manufacturing (Others)",
    "Electricity, Gas & Water companies ": "Electricity, Gas & Water companies",
    "Manufacturing (Food stuffs) ": "Manufacturing (Food stuffs)",
    "Manufacturing (Leather & products thereof) ": "Manufacturing (Leather & products thereof)",
    "Manufacturing (Machinery & Equipments) ": "Manufacturing (Machinery & Equipments)",
    "Mining & Quarrying ": "Mining & Quarrying",
    "Manufacturing (Wood Products) ": "Manufacturing (Wood Products)",
    "Insurance ": "Insurance",
    "Manufacture of other textiles/textile products": "",
    "CONSTRUCTION": "Construction",
}
state_mapp = {
    "Daman & Diu": "The Dadra And Nagar Haveli And Daman And Diu",
    "Chattisgarh": "Chhattisgarh",
    "Orissa": "Odisha",
    "Not assigned/MH": "Maharashtra",
    "Pondicherry": "Puducherry",
    "Jammu & Kashmir": "Jammu And Kashmir",
    "Andaman & Nicobar": "Andaman And Nicobar Islands",
    "Dadra & Nagar Haveli": "The Dadra And Nagar Haveli And Daman And Diu",
    "Daman and Diu": "The Dadra And Nagar Haveli And Daman And Diu",
    "Dadar Nagar Haveli": "The Dadra And Nagar Haveli And Daman And Diu",
    "Jammu and Kashmir": "Jammu And Kashmir",
    "nan": "Not Available",
    np.nan: "Not Available",
    "Chhaattisgarh": "Chhattisgarh",
    "Andaman & Nicobar Islands": "Andaman And Nicobar Islands",
    "Lucknow": "Uttar Pradesh",
    "Srinagar": "Jammu And Kashmir",
    "Andra Pradesh": "Andhra Pradesh",
    "Gujrat": "Gujarat",
    "MH": "Maharashtra",
    "TG": "Telangana",
    "GJ": "Gujarat",
    "CH": "Chandigarh",
    "DL": "Delhi",
    "HR": "Haryana",
    "UP": "Uttar Pradesh",
    "RJ": "Rajasthan",
    "CT": "Chhattisgarh",
    "KL": "Kerala",
    "WB": "West Bengal",
    "KA": "Karnataka",
    "MP": "Madhya Pradesh",
    "PB": "Punjab",
    "BR": "Bihar",
    "MN": "Manipur",
    "TN": "Tamil Nadu",
    "OR": "Odisha",
    "HP": "Himachal Pradesh",
    "UR": "Uttarakhand",
    "JH": "Jharkhand",
    "AP": "Andhra Pradesh",
    "GA": "Goa",
    "AS": "Assam",
    "DN": "Gujarat",
    "TR": "Tripura",
    "JK": "Jammu And Kashmir",
    "PY": "Puducherry",
    "MZ": "Mizoram",
    "NL": "Nagaland",
    "AN": "Andaman And Nicobar Islands",
    "AR": "Arunachal Pradesh",
    "LD": "Lakshadweep",
    "ML": "Meghalaya",
    "LH": "Jammu And Kashmir",
    "Kolkata": "West Bengal",
    "Pune": "Maharashtra",
    "Bangalore": "Karnataka",
    "Mumbai": "Maharashtra",
    "Ahmedabad": "Gujarat",
    "Kanpur": "Uttar Pradesh",
    "Gwalior": "Madhya Pradesh",
    "Chennai": "Tamil Nadu",
    "Hyderabad": "Telangana",
    "Ernakulam": "Kerala",
    "Coimbatore": "Tamil Nadu",
    "Jaipur": "Rajasthan",
    "Patna": "Bihar",
    "Shillong": "Meghalaya",
    "Telagana": "Telangana",
    "Maharastra": "Maharashtra",
    "Uttarakand": "Uttarakhand",
    "Jharkand": "Jharkhand",
    "Tamilnadu": "Tamil Nadu",
    "Uttar pradesh": "Uttar Pradesh",
}
roc_mapp = {
    "ROC - HYDERABAD": "Roc-Hyderabad",
    "ROC-PONDICHERRY": "Roc-Puducherry",
    "ROC - PATNA": "Roc-Patna",
    "ROC-HIMACHALPRADESH": "Roc-Himachal Pradesh",
    "ROC - CHHATTISGARH": "Roc-Chhattisgarh",
    "ROC - DELHI": "Roc-Delhi",
    "ROC - AHMEDABAD": "Roc-Ahmedabad",
    "ROC - HIMACHAL PRADE": "Roc-Himachal Pradesh",
    "ROC - JHARKHAND": "Roc-Jharkhand",
    "ROC - BANGALORE": "Roc-Bangalore",
    "ROC - ERNAKULAM": "Roc-Ernakulam",
    "ROC - MUMBAI": "Roc-Mumbai",
    "ROC - GWALIOR": "Roc-Gwalior",
    "ROC - CHANDIGARH": "Roc-Chandigarh",
    "ROC - PUNE": "Roc-Pune",
    "ROC - JAIPUR": "Roc-Jaipur",
    "ROC - CHENNAI": "Roc-Chennai",
    "ROC - KANPUR": "Roc-Kanpur",
    "ROC - KOLKATA": "Roc-Kolkata",
    "ROC - CUTTACK": "Roc-Cuttack",
    "ROC - SHILLONG": "Roc-Shillong",
    "ROC - UTTARAKHAND": "Roc-Uttarakhand",
    "ROC - COIMBATORE": "Roc-Coimbatore",
    "ROC - GOA": "Roc-Goa",
    "ROC - PONDICHERRY": "Roc-Puducherry",
    "ROC - JAMMU": "Roc-Jammu",
    "ROC - ANDAMAN": "Roc-Andaman",
    "ROC - HIMACHAL PRADESH": "Roc-Himachal Pradesh",
    "RoC - Hyderabad": "Roc-Hyderabad",
    "RoC - Mumbai": "Roc-Mumbai",
    "RoC - Delhi": "Roc-Delhi",
    "RoC - Pune": "Roc-Pune",
    "RoC - Kanpur": "Roc-Kanpur",
    "RoC - Bangalore": "Roc-Bangalore",
    "RoC - Ahmedabad": "Roc-Ahmedabad",
    "RoC - Ernakulam": "Roc-Ernakulam",
    "RoC - Coimbatore": "Roc-Coimbatore",
    "RoC - Cuttack": "Roc-Cuttack",
    "RoC - Chennai": "Roc-Chennai",
    "RoC - Chhattisgarh": "Roc-Chhattisgarh",
    "RoC - Uttarakhand": "Roc-Uttarakhand",
    "RoC - Jaipur": "Roc-Jaipur",
    "nan": "Not Available",
    "RoC - Patna": "Roc-Patna",
    "RoC - Jharkhand": "Roc-Jharkhand",
    "RoC - Kolkata": "Roc-Kolkata",
    "RoC - Shillong": "Roc-Shillong",
    "RoC - Gwalior": "Roc-Gwalior",
    "RoC - Himachal Pradesh": "Roc-Himachal Pradesh",
    "RoC - Chandigarh": "Roc-Chandigarh",
    "RoC - Pondicherry": "Roc-Puducherry",
    "RoC - Goa": "Roc-Goa",
    "RoC - Jammu": "Roc-Jammu",
    "RoC - Andaman": "Roc-Andaman",
    "RoC-HimachalPradesh": "Roc-Himachal Pradesh",
    "RoC-Pondicherry": "Roc-Puducherry",
    "ROC - VIJAYAWADA": "Roc-Vijayawada",
    "None": "Not Available",
    np.nan: "Not Available",
    "ROC-PONDICHERRY": "Roc-Puducherry",
    "ROC Mumbai": "Roc-Mumbai",
    "ROC Kolkata": "Roc-Kolkata",
    "ROC Shillong": "Roc-Shillong",
    "ROC Jammu": "Roc-Jammu",
    "ROC Ernakulam": "Roc-Ernakulam",
    "ROC Pune": "Roc-Pune",
    "ROC Delhi": "Roc-Delhi",
    "ROC Goa": "Roc-Goa",
    "ROC Chennai": "Roc-Chennai",
    "ROC Kanpur": "Roc-Kanpur",
    "ROC Hyderabad": "Roc-Hyderabad",
    "ROC Patna": "Roc-Patna",
    "ROC Vijayawada": "Roc-Vijayawada",
    "ROC Ahmedabad": "Roc-Ahmedabad",
    "ROC Jaipur": "Roc-Jaipur",
    "ROC Bangalore": "Roc-Bangalore",
    "ROC Coimbatore": "Roc-Coimbatore",
    "ROC Uttarakhand": "Roc-Uttarakhand",
    "ROC Chandigarh": "Roc-Chandigarh",
    "ROC Gwalior": "Roc-Gwalior",
    "ROC Jharkhand": "Roc-Jharkhand",
    "ROC Cuttack": "Roc-Cuttack",
    "ROC Chhattisgarh": "Roc-Chhattisgarh",
    "ROC Himachal Pradesh": "Roc-Himachal Pradesh",
    "ROC Pondicherry": "Roc-Puducherry",
    "ROC Andaman": "Roc-Andaman",
    "Roc-Jharkandarkhand": "Roc-Jharkhand",
    "Roc - Mumbai": "Roc-Mumbai",
    "Roc-Vijayawada": "Roc-Vijayawada",
    "Roc Guwahati": "Roc-Guwahati",
    "Roc Cum Ol Patna": "Roc-Patna",
    "Roc Cum Ol Jaipur": "Roc-Jaipur",
    "Roc Cum Ol Goa": "Roc-Goa",
    "Roc Cum Ol Cuttack": "Roc-Cuttack",
    "Roc Puducherry": "Roc-Puducherry",
    "Roc Cum Ol Shimla": "Roc-Shimla",
    "Roc Cum Ol Bilaspur": "Roc-Bilaspur",
    "Roc Cum Ol Ranchi": "Roc-Ranchi",
    "Roc Cum Ol Dehradun": "Roc-Dehradun",
    "Roc Cum Ol Jammu": "Roc-Jammu",
}
status_mapp = {
    "ACTV": "Active",
    "CAPT": "Capitalized",
    "Not available for efiling": "Not Available for E-filing",
    np.nan: None,
    "Not Available for Efiling": "Not Available for E-Filing",
}
class_mapp = {
    "OPC": "One Person Company",
    "Private(One Person C": "One Person Company",
    "Private - One Person": "One Person Company",
    "Private-One person": "One Person Company",
    "Private-One Person": "One Person Company",
    "Private - One person": "One Person Company",
    "One Person": "One Person Company",
    "Private One Person": "One Person Company",
    "Private Company": "One Person Company",
    "Private One Person Company": "One Person Company",
    "Public Company": "Public",
    "Private-One Company": "One Person Company",
    "Private One Company": "One Person Company",
    "Private(One Person Company)": "One Person Company",
    "One Person Company": "One Person Company",
    np.nan: None,
    "Publlic": "Public",
}
cat_mapp = {
    "Companies Limited by Shares": "Company Limited by Shares",
    "Companies Limited by Guarantee": "Company Limited by Guarantee",
    np.nan: None,
    "Company Limted by Guarantee": "Company Limited by Guarantee",
    "Company Limted by Shares": "Company Limited by Shares",
    "Company Limjted By Shares": "Company Limited by Shares",
    "Company Limjted By Guarentee": "Company Limited by Guarantee",
    "Company Limited By Share": "Company Limited by Shares",
    "Company Limited By Guarentee": "Company Limited by Guarantee",
}
sub_cat_mapp = {
    "Non-govt company": "Non-Government Company",
    "Guarantee and Association comp": "Guarantee and association Company",
    "State Govt company": "State Government Company",
    "Union Govt company": "Union Government Company",
    "Indian Non-Government Company": "Non-Government Company",
}
type_mapp = {
    "Non-Government": "Non-Government Company",
    "Government": "Government Company",
    "Nongovernment": "Non-Government Company",
    "Non Government": "Non-Government Company",
    "Non-Governement": "Non-Government Company",
    "Governement": "Government Company",
    "Non-Government Company": "Non-Government Company",
    "Government Company": "Government Company",
    "Non- Government": "Non-Government Company",
    "Non-govt company": "Non-Government Company",
    "Subsidiary of Foreign Company": "Subsidiary of Foreign Company",
    "State Govt company": "State Government Company",
    "Guarantee and Association comp": "Guarantee and association Company",
    "Union Govt company": "Union Government Company",
    " Guarantee and Association comp": "Guarantee and association Company",
    "Non-Govt.": "Non-Government Company",
    "Govt.": "Government Company",
    "Non-Govt": "Non-Government Company",
    "Govt": "Government Company",
    "Guarantee And Association Comp": "Guarantee and association Company",
    "Non-government": "Non-Government Company",
    "Non Government Company": "Non-Government Company",
    "Guarentee Associated Company": "Guarantee and association Company",
    "State Government Company": "State Government Company",
    "Gaurentee Associated Company": "Guarantee and association Company",
    "Union Government Company": "Union Government Company",
    "Non govt Company": "Non-Government Company",
    "State govt Company": "State Government Company",
    "Non-govt company ": "Non-Government Company",
    "Subsidiary of Foreign Company ": "Subsidiary of Foreign Company",
    "Union Govt company ": "Union Government Company",
    "State Govt company ": "State Government Company",
    "Guarantee and Association comp ": "Guarantee and association Company",
    "Subcidary of Foreign Company": "Subsidiary of Foreign Company",
    "State Government company": "State Government Company",
    "Subcidery of Foreing Company": "Subsidiary of Foreign Company",
    "Non-government company": "Non-Government Company",
    "subsidiary of company incorporated outside India": "Subsidiary of Foreign Company",
    "Guarantee and association Company": "Guarantee and association Company",
    "Union government company": "Union Government Company",
    "State government company": "State Government Company",
    "Non-Govt Company": "Non-Government Company",
    "Union Govt Company": "Union Government Company",
    "State Govt Company": "State Government Company",
    "Guarantee And Association Comp": "Guarantee and association Company",
}
# %% IC Consolidation
# get all files in interim folder starting with ic using glob
ic_files = glob.glob(
    os.path.join(interim_path, "old format", "**", "ic_*"), recursive=True
)
# Initialize an empty list to store DataFrames
dfs = []
cols = [
    "registration_date",
    "cin",
    "state_name",
    "roc",
    "company_status",
    "company_name",
    "company_nature",
    "company_sub_category",
    "company_class",
    "company_category",
    "company_type",
    "company_address",
    "company_email",
    "authorized_capital",
    "paidup_capital",
    "act_code",
    "act_description",
]

# Read and process each file
for file_path in ic_files:
    file_name = file_path.split("\\")[-1]
    # print(f"\033[1;32mFile: {file_name}\033[0m")
    df = pd.read_csv(file_path)
    df.rename(columns=col_rename, inplace=True)
    df.reset_index(drop=True, inplace=True)
    # select only columns in cols if not in df then append it
    for col in cols:
        if col not in df.columns:
            df[col] = None
    df = df.loc[:, cols]
    # print(df.shape)
    # Remove rows having all nan values
    df.dropna(how="all", axis=0, inplace=True)
    # print(df.shape)
    dfs.append(df)

# Concatenate all DataFrames
ic = pd.concat(dfs, ignore_index=True)
ic.reset_index(drop=True, inplace=True)

ic["act_description"] = ic["act_description"].replace(desc_rename)
# Assign the descriptions to the codes
ic["act_description"] = ic["act_code"].map(unique_code_mapp)
ic["state_name"] = ic["state_name"].replace(state_mapp).str.title().str.strip()
ic["roc"] = ic["roc"].replace(roc_mapp).str.title().str.strip()
# assign company_nature to company_type if company_type is nan
ic.loc[ic["company_type"].isna(), "company_type"] = ic.loc[
    ic["company_type"].isna(), "company_nature"
]
ic["company_status"] = (
    ic["company_status"].replace(status_mapp).str.title().str.strip()
)
ic["company_class"] = (
    ic["company_class"].replace(class_mapp).str.title().str.strip()
)
ic["company_category"] = (
    ic["company_category"].replace(cat_mapp).str.title().str.strip()
)
ic["company_sub_category"] = (
    ic["company_sub_category"].replace(sub_cat_mapp).str.title().str.strip()
)
ic["company_type"] = (
    ic["company_type"].replace(type_mapp).str.title().str.strip()
)
# assign company_nature to company_type if company_type is nan
ic.loc[ic["company_type"].isna(), "company_type"] = ic.loc[
    ic["company_type"].isna(), "company_sub_category"
]
# Drop rows where registraion_date is nan
ic.dropna(subset=["registration_date"], inplace=True)
ic["formatted_date"] = ic["registration_date"].apply(parse_date)
# Create new column state_code by mapping state_name from ic to lgd_codes state_name and assign state_code
ic["state_code"] = ic["state_name"].map(
    lgd_codes.set_index("state_name")["state_code"]
)
# state_code should be string with zfill of 2 else None
ic["state_code"] = ic["state_code"].apply(
    lambda x: str(int(float(x))).zfill(2) if pd.notnull(x) else "Not Available"
)
# Drop columns company_nature, company_sub_category, act_code and registration_date
ic.drop(
    columns=[
        "company_nature",
        "company_sub_category",
        "act_code",
        "registration_date",
    ],
    inplace=True,
)
ic.rename(columns={"formatted_date": "registration_date"}, inplace=True)
ic = ic[
    [
        "registration_date",
        "cin",
        "state_name",
        "state_code",
        "roc",
        "company_name",
        "company_status",
        "company_type",
        "company_class",
        "company_category",
        "act_description",
        "company_address",
        "company_email",
        "authorized_capital",
        "paidup_capital",
    ]
]
# convert authorized_capital and paidup_capital to numeric if nan then convert to None
ic["authorized_capital"] = pd.to_numeric(
    ic["authorized_capital"], errors="coerce"
)
ic["paidup_capital"] = pd.to_numeric(ic["paidup_capital"], errors="coerce")
# replace nan with None in ic
ic.replace(np.nan, None, inplace=True)
# Assign values
ic.loc[ic["company_name"].isna(), "company_name"] = (
    "BINDU FASHION PRIVATE LIMITED"
)
ic.loc[ic["cin"].isna(), "cin"] = "U74999MH2017PTC296851"
ic.loc[ic["company_category"].isna(), "company_category"] = (
    "Company Limited by Shares"
)
ic.loc[ic["company_type"].isna(), "company_type"] = "Non-Government Company"

ic["state_code"] = ic["state_code"].apply(
    lambda x: "Not Available" if x is None else x
)
ic["company_status"] = ic["company_status"].apply(
    lambda x: "Not Available" if x is None else x
)
ic["act_description"] = ic["act_description"].apply(
    lambda x: "Not Available" if x is None else x
)
ic["company_email"] = ic["company_email"].apply(
    lambda x: "Not Available" if x is None else x
)
ic["company_address"] = ic["company_address"].apply(
    lambda x: "Not Available" if x is None else x
)
ic.replace(np.nan, None, inplace=True)
# %%
# Save ic to csv
ic.to_csv(os.path.join(interim_path, "ic_consolidated_old.csv"), index=False)
ic.to_parquet(
    os.path.join(interim_path, "ic_consolidated_old.parquet"), index=False
)

# %% LLP consolidation
llp_files = glob.glob(
    os.path.join(interim_path, "old format", "**", "llp_*"), recursive=True
)
llp_cols_rename = {
    "month name": "month",
    "state": "state_name",
    "no. of designated partners": "designated_partners",
    "number of partners": "total_partners",
    "roc": "roc",
    "subdescription": "act_sub_description",
    "email": "company_email",
    "description activity": "act_description",
    "activity code": "act_code",
    "llp status": "llp_status",
    "number of designated partners": "designated_partners",
    "activity description": "act_description",
    "status": "llp_status",
    "registered_office_address": "company_address",
    "registered office address": "company_address",
    "obligation of contribution": "obligation_of_contribution",
    "activity desc": "act_description",
    "no. of partners": "total_partners",
    "main discription": "act_description",
    "industrial activity code": "act_code",
    "founded": "incorporation_date",
    "industrial activity description": "act_description",
    "roc location": "roc",
    "description \n(5 digit discription)": "sub_discription",
    "total obligation of contribution": "obligation_of_contribution",
    "obligation of contribution(rs.)": "obligation_of_contribution",
    "limited liability partnership name": "llp_name",
    "district": "district_name",
    "date of incorporation": "incorporation_date",
    "address": "company_address",
    "description": "act_description",
    "llp name": "llp_name",
    "llpin": "llpin",
    "industrial activity": "act_code",
}

llp_dfs = []
llp_cols = [
    "state_name",
    "roc",
    "incorporation_date",
    "llp_name",
    "llpin",
    "llp_status",
    "designated_partners",
    "total_partners",
    "obligation_of_contribution",
    "act_code",
    "act_description",
    "act_sub_description",
    "company_email",
    "company_address",
]
for file_path in llp_files:
    file_name = file_path.split("\\")[-1]
    print(f"\033[1;32mFile: {file_name}\033[0m")
    df = pd.read_csv(file_path)
    df.rename(columns=llp_cols_rename, inplace=True)
    df.reset_index(drop=True, inplace=True)
    print(df.columns)
    # select only columns in cols if not in df then append it
    for col in llp_cols:
        if col not in df.columns:
            print(f"\033[1;31mColumn {col} not found in {file_name}\033[0m")
            df[col] = None
    df = df.loc[:, llp_cols]
    print(df.shape)
    # Remove rows having all nan values
    df.dropna(how="all", axis=0, inplace=True)
    df.reset_index(drop=True, inplace=True)
    print(df.columns)
    llp_dfs.append(df)

# Concatenate all DataFrames
llp = pd.concat(llp_dfs, ignore_index=True)
llp.reset_index(drop=True, inplace=True)

llp["act_code"] = llp["act_code"].replace(np.nan, None)
# llp['act_code'] = llp['act_code'].apply(lambda x: str(int(x)) if x is not None else x)
# assign act_description in llp by mapping act_code from unique_code_mapp to llp
llp["act_description"] = llp["act_code"].map(unique_code_mapp)

llp["state_name"] = (
    llp["state_name"].replace(state_mapp).str.title().str.strip()
)
llp["roc"] = llp["roc"].replace(roc_mapp).str.title().str.strip()

llp["llp_status"] = (
    llp["llp_status"].replace(status_mapp).str.title().str.strip()
)
# drop rows where incorporation_date is nan
llp.dropna(subset=["incorporation_date"], inplace=True)
llp.reset_index(drop=True, inplace=True)
# drop columns act_code and act_sub_description
llp.drop(columns=["act_code", "act_sub_description"], inplace=True)
llp["incorporation_date"] = llp["incorporation_date"].apply(parse_date)

# Create new column state_code by mapping state_name from ic to lgd_codes state_name and assign state_code
llp["state_code"] = llp["state_name"].map(
    lgd_codes.set_index("state_name")["state_code"]
)
# state_code should be string with zfill of 2 else None
llp["state_code"] = llp["state_code"].apply(
    lambda x: str(int(float(x))).zfill(2) if pd.notnull(x) else "Not Available"
)

llp = llp[
    [
        "incorporation_date",
        "state_name",
        "state_code",
        "roc",
        "llp_name",
        "llpin",
        "llp_status",
        "designated_partners",
        "total_partners",
        "obligation_of_contribution",
        "act_description",
        "company_email",
        "company_address",
    ]
]

llp.replace(np.nan, None, inplace=True)

llp["obligation_of_contribution"] = pd.to_numeric(
    llp["obligation_of_contribution"], errors="coerce"
)
llp["designated_partners"] = pd.to_numeric(
    llp["designated_partners"], errors="coerce"
)
llp["total_partners"] = pd.to_numeric(llp["total_partners"], errors="coerce")

str_cols = [col for col in llp.columns if llp[col].dtype == "object"]
for col in str_cols:
    llp[col] = llp[col].astype(str)

llp["state_name"] = (
    llp["state_name"]
    .replace("Not Assigned", "Not Available")
    .str.title()
    .str.strip()
)
llp["llp_status"] = llp["llp_status"].apply(
    lambda x: "Not Available" if x is None else x
)
llp["act_description"] = llp["act_description"].apply(
    lambda x: "Not Available" if x is None else x
)
llp["company_email"] = llp["company_email"].apply(
    lambda x: "Not Available" if x is None else x
)
llp["company_address"] = llp["company_address"].apply(
    lambda x: "Not Available" if x is None else x
)
llp.replace(np.nan, None, inplace=True)
# %%
# Save ic to csv
llp.to_csv(os.path.join(interim_path, "llp_consolidated_old.csv"), index=False)
llp.to_parquet(
    os.path.join(interim_path, "llp_consolidated_old.parquet"), index=False
)

# %% Foreign Company consolidation
fc_files = glob.glob(
    os.path.join(interim_path, "old format", "**", "foreign_*"), recursive=True
)

fc_cols_rename = {
    "Status": "company_status",
    "company status": "company_status",
    "month name": "month",
    "state": "state_name",
    "Date": "registration_date",
    "MONTH NAME": "month",
    "company_status": "company_status",
    "State": "state_name",
    "EMAIL": "company_email",
    "roc": "roc",
    "S.No": "sn",
    "subdescription": "act_sub_description",
    "month_name": "month",
    "date_of_registration": "registration_date",
    "foreign company present address in india": "company_address_india",
    "COMPANY NAME": "company_name",
    "email": "company_email",
    "REGISTERED OFFICE ADDRESS": "company_address_india",
    "foreign office address": "company_address_foreign",
    "activity code": "act_code",
    "ROC": "roc",
    "Activity": "act_code",
    "type of office": "office_type",
    "CIN": "fcin",
    "activity description": "act_description",
    "company name": "company_name",
    "status": "company_status",
    "Description": "act_description",
    "activity_code": "act_code",
    "registered office address": "company_address_india",
    "registered_office_address": "company_address_india",
    "DATE OF REGISTRATION": "registration_date",
    "ACTIVITY CODE": "act_code",
    "TYPE OF OFFICE": "office_type",
    "ACTIVITY DESCRIPTION": "act_description",
    "activty description": "act_description",
    "type": "company_type",
    "Email": "company_email",
    "activity": "act_code",
    "Office Type": "office_type",
    "country of incorporation": "incorporation_country",
    "COMPANY STATUS": "company_status",
    "activity_description": "act_description",
    "cin": "fcin",
    "office type": "office_type",
    "fcin": "fcin",
    "address": "company_address_india",
    "description": "act_description",
    "fcrn": "fcin",
    "date": "registration_date",
    "FCIN": "fcin",
    "date of registration": "registration_date",
    "Address": "company_address_india",
    "STATE": "state_name",
    "company_name": "company_name",
    "Company Name": "company_name",
}

fc_dfs = []
fc_cols = [
    "registration_date",
    "fcin",
    "state_name",
    "roc",
    "company_name",
    "company_type",
    "company_status",
    "act_code",
    "act_description",
    "act_sub_description",
    "office_type",
    "company_email",
    "company_address_india",
    "company_address_foreign",
    "incorporation_country",
]

# Read and process each file
for file_path in fc_files:
    file_name = file_path.split("\\")[-1]
    print(f"\033[1;32mFile: {file_name}\033[0m")
    df = pd.read_csv(file_path)
    df.rename(columns=fc_cols_rename, inplace=True)
    df.reset_index(drop=True, inplace=True)
    print(df.columns)
    # select only columns in cols if not in df then append it
    for col in fc_cols:
        if col not in df.columns:
            print(f"\033[1;31mColumn {col} not found in {file_name}\033[0m")
            df[col] = None
    df = df.loc[:, fc_cols]
    print(df.shape)
    # Remove rows having all nan values
    df.dropna(how="all", axis=0, inplace=True)
    df.reset_index(drop=True, inplace=True)
    print(df.columns)
    fc_dfs.append(df)

# Concatenate all DataFrames
fc = pd.concat(fc_dfs, ignore_index=True)
fc.reset_index(drop=True, inplace=True)

fc["act_code"] = fc["act_code"].replace(np.nan, None)
fc["act_code"] = fc["act_code"].apply(
    lambda x: str(int(x)) if x is not None else x
)
# assign act_description in llp by mapping act_code from unique_code_mapp to llp
fc["act_description"] = fc["act_code"].map(unique_code_mapp)
fc["state_name"] = fc["state_name"].replace(state_mapp).str.title().str.strip()
fc["roc"] = fc["roc"].replace(roc_mapp).str.title().str.strip()
fc["company_status"] = (
    fc["company_status"].replace(status_mapp).str.title().str.strip()
)
# drop rows where incorporation_date is nan
fc.dropna(subset=["registration_date"], inplace=True)
fc.reset_index(drop=True, inplace=True)

fc["registration_date"] = fc["registration_date"].apply(parse_date)

fc.replace(np.nan, "Not Available", regex=True, inplace=True)
fc.drop(
    columns=[
        "act_code",
        "act_sub_description",
        "company_type",
        "company_address_foreign",
        "incorporation_country",
    ],
    inplace=True,
)

office_type_renmae = {
    "Liasion Office": "LiaIson Office",
    "Branch Office": "Branch Office",
    "Project Office": "Project Office",
    "BO - Branch Office": "Branch Office",
    "Liaison Office": "Liaison Office",
    "Representative Office": "Representative Office",
    "Other Office": "Other Office",
    "LO": "Liaison Office",
    "PO": "Project Office",
    "Not Available": "Not Available",
    "Registrar of Companies, National Capital Territory of Delhi and Haryana": "RoC, Delhi & Haryana",
}

fc["office_type"] = (
    fc["office_type"].replace(office_type_renmae).str.title().str.strip()
)

# Create new column state_code by mapping state_name from ic to lgd_codes state_name and assign state_code
fc["state_code"] = fc["state_name"].map(
    lgd_codes.set_index("state_name")["state_code"]
)
# state_code should be string with zfill of 2 else None
fc["state_code"] = fc["state_code"].apply(
    lambda x: str(int(float(x))).zfill(2) if pd.notnull(x) else None
)

fc.rename(columns={"company_address_india": "company_address"}, inplace=True)
fc = fc[
    [
        "registration_date",
        "fcin",
        "state_name",
        "state_code",
        "roc",
        "company_name",
        "company_status",
        "act_description",
        "office_type",
        "company_email",
        "company_address",
    ]
]

str_col = ["fcin", "company_name", "company_address", "company_email"]
for col in str_col:
    fc[col] = fc[col].astype(str)
# %%
# Save ic to csv
fc.to_csv(os.path.join(interim_path, "fc_consolidated_old.csv"), index=False)
fc.to_parquet(
    os.path.join(interim_path, "fc_consolidated_old.parquet"), index=False
)
