# import libraries
import os
import re
from pathlib import Path

import pandas as pd

# set paths
dir_path = os.path.dirname(os.getcwd())
data_path = os.path.join(dir_path, "data")
raw_path = os.path.join(data_path, "raw")
interim_path = os.path.join(data_path, "interim")
processed_path = os.path.join(data_path, "processed")

# get file paths from raw folder
local_files = Path(raw_path).glob("*.xlsx")

df_list = []
for file in local_files:
    try:
        print("Processing:", file)
        df = pd.read_excel(file).dropna(how="all", axis=1)
        df = df[3:]
        df = df.fillna(method="ffill", axis=1)
        # Select columns
        filtered = df[[df.columns.tolist()[0], df.columns.tolist()[2]]].copy()
        filtered.columns = ["type", "value"]
        # Remove rows with NaN
        filtered = filtered.dropna(subset=["value"]).reset_index(drop=True)
        # Extract Date of Data
        year = filtered.value.iloc[0]
        date = re.findall(r"(\w+)", filtered["value"].iloc[1])
        data_date = date[1] + "-" + str(date[0]) + "-" + str(year)
        # Remove rows with NaN
        filtered = filtered.dropna(subset=["type"]).reset_index(drop=True)
        filtered["type"] = filtered["type"].str.replace(r"[^a-zA-z0-9 ]", " ")
        filtered["date"] = data_date
        # Save as csv file in interim folder
        file_name = file.split("\\")[-1].split(".")[0]
        interim_csv_path = os.path.join(interim_path, f"{file_name}.csv")
        filtered.to_csv(interim_csv_path, index=False)
        print("Processed:", file)
        df_list.append(filtered)

    except Exception as e:
        print("Error:", file, e)

refined_dfs = []
for df in df_list:
    # Pivot table with date as index and type as columns
    pivoted = (
        df.pivot(index="date", columns="type", values="value")
        .copy()
        .reset_index()
    )
    # Rename columns
    for col in pivoted.columns:
        if "Currency with the Public" in col:
            pivoted.rename(columns={col: "currency_with_public"}, inplace=True)
        elif "Demand Deposits with Banks" in col:
            pivoted.rename(
                columns={col: "demand_deposits_with_banks"}, inplace=True
            )
        elif "Time Deposits with Banks" in col:
            pivoted.rename(
                columns={col: "time_deposits_with_banks"}, inplace=True
            )
        elif "Deposits with Reserve Bank" in col:
            pivoted.rename(
                columns={col: "other_deposits_with_rbi"}, inplace=True
            )
        elif "M3" in col:
            pivoted.rename(columns={col: "m3"}, inplace=True)
    # Select columns to keep
    pivoted = pivoted[
        [
            "date",
            "currency_with_public",
            "demand_deposits_with_banks",
            "time_deposits_with_banks",
            "other_deposits_with_rbi",
            "m3",
        ]
    ]
    refined_dfs.append(pivoted)

# Concatenate all dataframes
data = pd.concat(refined_dfs).reset_index(drop=True)
# Drop duplicates
data = data.drop_duplicates()
# Drop duplicates based on date
data = data.drop_duplicates(subset=["date"])
# Fix date format
data.loc[data["date"] == "19-July-2019", "date"] = "19-Jul-2019"
# Convert date to datetime format and then to %d-%m-%Y
data["date"] = pd.to_datetime(data["date"], format="%d-%b-%Y").dt.strftime(
    "%d-%m-%Y"
)
# Convert numeric columns to float
num_cols = [
    "currency_with_public",
    "demand_deposits_with_banks",
    "time_deposits_with_banks",
    "other_deposits_with_rbi",
    "m3",
]
for col in num_cols:
    # Remove commas from column and then convert it to float
    data[col] = data[col].replace(",", "", regex=True).astype(float)

# Save as csv file in processed folder
processed_csv_path = os.path.join(processed_path, "rbi_money_stocks.csv")
data.to_csv(processed_csv_path, index=False)
