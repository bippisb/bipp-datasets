# Importing necessary modules

from pathlib import Path

import pandas as pd

# Storing directory paths

dir_path = Path.cwd()
raw_data_path = Path.joinpath(dir_path, "data", "raw")
interim_data_path = Path.joinpath(dir_path, "data", "interim")
proc_data_path = Path.joinpath(dir_path, "data", "processed")

# Reading datasets in dataframe format

sfac = pd.read_csv(Path.joinpath(interim_data_path, "sfac.csv"))
non_sfac = pd.read_csv(Path.joinpath(interim_data_path, "non_sfac.csv"))

month = {
    "Jan": "01",
    "Feb": "02",
    "Mar": "03",
    "Apr": "04",
    "May": "05",
    "Mai": "05",
    "Jun": "06",
    "Jul": "07",
    "Aug": "08",
    "Sep": "09",
    "Okt": "10",
    "Oct": "10",
    "Nov": "11",
    "Dez": "12",
    "Dec": "12",
}

# SFAC

# Storing "reg_date" in a "date" variable and creating separate "regn_date" column

date = sfac["reg_date"]

sfac["regn_date"] = ""

# Collecting registration date from date variable, converting into proper format and adding to the new date column "regn_date"

for i in range(len(date)):
    if type(date[i]) == str:
        d_list = date[i].split(".")
        if len(d_list) == 3:
            if d_list[1] in month:
                m = month[d_list[1]]
            else:
                if d_list[1][-1] == "r":
                    m = "03"
                elif (d_list[1][0] == "J") and (d_list[1][-1] == "n"):
                    m = "01"
            d = str(d_list[0]) + "/" + str(m) + "/" + str(d_list[2])
            sfac["regn_date"][i] = d

# Dropping the old date column and saving to csv

sfac.drop(["reg_date"], axis=1, inplace=True)
sfac.to_csv(Path.joinpath(proc_data_path, "sfac.csv"), index=False)

# NON-SFAC

# Storing "reg_date" in a "date" variable and creating separate "regn_date" column

date = non_sfac["reg_date"]
non_sfac["regn_date"] = ""

# Collecting registration date from date variable, converting into proper format and adding to the new date column "regn_date"

for i in range(len(date)):
    if type(date[i]) == str:
        d_list = date[i].split(".")
        if len(d_list) == 3:
            if d_list[1] in month:
                m = month[d_list[1]]
            else:
                if (d_list[1][-1] == "r") and (d_list[1][0] == "M"):
                    m = "03"
                elif (d_list[1][0] == "J") and (d_list[1][-1] == "n"):
                    m = "01"
            d = str(d_list[0]) + "/" + str(m) + "/" + str(d_list[2])
            non_sfac["regn_date"][i] = d
        else:
            l1 = date[i].split(" ")
            d = l1[0]
            non_sfac["regn_date"][i] = d

# Dropping the old date column and saving to csv

non_sfac.drop(["reg_date"], axis=1, inplace=True)
non_sfac.to_csv(Path.joinpath(proc_data_path, "non_sfac.csv"), index=False)
