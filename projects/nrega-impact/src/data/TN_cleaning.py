import numpy as np
import pandas as pd

# Cleaning and Importing TN data
TN = pd.read_csv("data/raw/NREGA_assets_raw/RAJASTHAN.csv")

# removing works with deleted work_status


def work_status_filter(data):
    data = data[data["work_status"].isin(["Completed", "Physically Completed"])]
    data = data.drop("work_name", axis=1)
    return data


TN = work_status_filter(TN)

# special state filters


def special_rows_filter(data):
    if any(data["state"] == "KERALA"):
        data = data[(data["block_name"] != "KURWAI")]
    elif any(data["state"] == "TELANGANA"):
        data = data[(data["panchayat_name"].notna())]

    return data


TN = special_rows_filter(TN)


# stripping tariling lines in strings


def trail_strip(data):
    for i in data.select_dtypes(include="object").columns.tolist():
        if any(data[i].str.contains("01-01-1900", regex=False)):
            data[i] = data[i].str.replace("01-01-1900", "")
        data[i] = data[i].str.rstrip()

    return data


TN = trail_strip(TN)


# removing digits and special characters from name strings
def nrega_string_clean(data):
    for i in ["panchayat_name", "block_name", "district", "state"]:
        meta_char = (
            r"\#|\&|\@|\$|\%|\^|\*|\(|\)|\)|\_|\+|\=|\\|\/|\?|\>|\<|\:|\;|\`|\~|\!"
        )
        if any(data[i].str.contains(r"\d+", regex=True)):
            data[i] = data[i].str.replace(r"\d+", "")
        if any(data[i].str.contains(meta_char)):
            data[i] = data[i].str.replace(meta_char, "")

    for i in [
        "master_work_category_name",
        "work_category_name",
        "work_type",
        "agency_name",
    ]:
        if any(data[i].str.contains(r"^<", regex=True)):
            data[i] = data[i].replace(r"<.*", np.nan, regex=True)

    return data


TN = nrega_string_clean(TN)


# rectifying date coumn types


def date_cleaner(data):
    for i in ["work_started_date", "work_physically_completed_date"]:
        if data[i].dtype == "O":
            if any(data[i].str.contains(r"[A-Z]|[a-z]", regex=True)):
                data[i] = data[i].str.replace(r"[A-Z]|[a-z]", "")
            data[i] = pd.to_datetime(data[i], errors="coerce")
        else:
            data[i] = pd.to_datetime(data[i], errors="coerce")
    # data[i]=pd.to_datetime(data[i], errors='coerce')
    return data


TN = date_cleaner(TN)

# identifying points where 'work_started_date'>'work_physically_completed_date'


def early_complete(data):
    conditions = [
        data["work_started_date"] > data["work_physically_completed_date"],
        data["work_started_date"] == data["work_physically_completed_date"],
        data["work_started_date"] < data["work_physically_completed_date"],
        data["work_physically_completed_date"].isna(),
        data["work_started_date"].isna(),
    ]
    options = [
        "Finished before start",
        "On the same day",
        "After start",
        "End date missing",
        "Start date missing",
    ]
    data["finished_when"] = np.select(conditions, options)
    return data


TN = early_complete(TN)

# ISID: Attempting to identify each row uniquely by a combination of column values


def isid(data, col_names):  # could avoid the col_names and give these cols as default
    data = data.set_index(col_names)
    if data.index.is_unique:
        data = data.reset_index()
        print("The selected columns make a unique key")
    else:
        print("The selected columns doesn't make a unique key")
        print("Checking for missing values in the coulmns")
        data = data.reset_index()
        for (
            i
        ) in col_names:  # checking presence of missing values in the concerend columns
            if data[i].isna().value_counts()[False] == len(data[i]):
                print(str("No missing values in " + i))
            else:
                print(
                    str("Missing values found in " + i)
                )  # didnt find any missing vals until now. If present add fucntion to deal with it
        print(
            "Checking for duplicates....."
        )  # checking presence of duplicate values in the concerend columns
        data["dups"] = data.groupby(
            col_names
        ).cumcount()  # creating duplicate identifier 'dups' to using grouped subsets of rows
        print(data["dups"].value_counts())  # Taking count of dups
        print("The following are a snapshot of duplicate values, at top 25 rows")
        cols_names = col_names + [
            "dups"
        ]  # Creating a second duplicate identifier so that dupicates can be displayed
        data["dups"] = data.groupby(col_names).cumcount()
        data["dups2"] = data.groupby(col_names)["dups"].transform(max)
        print(
            data.loc[(data["dups2"] > 0), cols_names].sort_values(cols_names).head(25)
        )
        print("The following are a snapshot of duplicate values, at bottom 25 rows")
        print(
            data.loc[(data["dups2"] > 0), cols_names].sort_values(cols_names).tail(25)
        )
        print("Removing the duplicates")
        # duplicates = data[data["dups2"] > 0].sort_values(col_names)
        # duplicates.to_csv("data/interim/uttarakhand_duplicates.csv", index=False)
        # print(duplicates)
        # data.loc[(data['dups2']==0), :]
        # data.drop(["dups", "dups2"], axis=1)
    return data


TN = isid(TN, ["block_name", "panchayat_name", "work_code", "work_started_date"])


# rearranging columns
def col_rearrange(data):
    cols_new = [
        "s_no",
        "state",
        "district",
        "block_name",
        "panchayat_name",
        "work_code",
        "work_started_date",
        "work_physically_completed_date",
        "finished_when",
        "sanction_amount_in_lakh",
        "total_amount_paid_since_inception_in_lakh",
        "total_mandays",
        "no_of_units",
        "is_secure",
        "work_status",
        "master_work_category_name",
        "work_category_name",
        "work_type",
        "agency_name",
        "work_start_fin_year",
    ]  # to put s.no as the first column. Happened after index reset in ISID
    data = data[cols_new]
    return data


TN = col_rearrange(TN)


TN.head(25)
TN.dtypes
TN.columns

TN.to_csv("data/interim/NREGA_assets/xxx.csv", index=False)
