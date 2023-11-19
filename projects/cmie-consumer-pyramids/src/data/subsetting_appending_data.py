from pathlib import Path

import numpy as np
import pandas as pd

# defining paths

dir_path = Path.cwd()
raw_data = Path.joinpath(dir_path, "data/raw")
files = list(raw_data.glob("*.csv"))

# defining a lsit to collect all df
df_collect = []

# looping through files for creating interim data

for file in files:

    # reading in the files
    df = pd.read_csv(file)

    # subsetiing for only food based consumption details
    df = df.iloc[:, 0:86]

    # lower-casing all var names
    var_names = [x.lower() for x in df.columns]
    df.columns = var_names

    # splitting year and month
    df[["month", "year"]] = df["month"].str.split(" ", expand=True)

    # removing all HH without any responses
    df = df[df["response_status"] != "Non-Response"]

    # making all 0 as NaN
    for i in df.columns:
        if df[i].dtype == "int64":
            conds = [df[i] == 0]

            opts = [np.nan]

            df[i] = np.select(conds, opts, default=df[i])

    # removing response_satus and reson for no response
    df = df.drop(
        [
            "response_status",
            "reason_for_non_response",
            "hr",
            "stratum",
            "psu_id",
            "month_slot",
        ],
        axis=1,
    )

    # removing & in state names
    df["state"] = df["state"].str.replace("&", "and")

    # rearranging columns

    # rearranging columns
    col_names = df.columns

    colnames_new = []

    colnames_new.extend(col_names[1:3])
    colnames_new.append(col_names[0])
    colnames_new.append(col_names[-1])
    colnames_new.append(col_names[4])
    colnames_new.append(col_names[3])
    colnames_new.extend(col_names[12 : len(col_names) - 1])
    colnames_new.extend(col_names[5:12])

    df = df[colnames_new]

    # isid for HH_ID
    if df["hh_id"].is_unique:
        print("HH IDs are unique for", df["month"].unique())

    # writing csv to interim
    month = df["month"].unique()[0]
    year = df["year"].unique()[0]
    interim_path = Path.joinpath(dir_path, "data/interim")
    csv_name = "/" + month + "_" + year + ".csv"
    df.to_csv(str(interim_path) + csv_name, index=False)

    print(month, year, "exported as csv to Interim directory")

    # appending df to df_collect
    df_collect.append(df)

print("Initiating the appending process")
# concatenating all df to a single dataset
final_df = pd.concat(df_collect)

# exporting final_df
final_df.to_csv(Path.joinpath(interim_path, "sep_2020_to_aug_2021.csv"), index=False)

print("All files have been exported. Loop rests.")
