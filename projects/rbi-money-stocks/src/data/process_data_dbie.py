# %%
import os

import pandas as pd

# %%
# set paths
dir_path = os.path.dirname(os.path.dirname(os.getcwd()))
data_path = os.path.join(dir_path, "data")
raw_path = os.path.join(data_path, "raw")
interim_path = os.path.join(data_path, "interim")
processed_path = os.path.join(data_path, "processed")
# %%
# get file paths from raw folder
local_files = [
    os.path.join(raw_path, f)
    for f in os.listdir(raw_path)
    if f.endswith(".xlsx")
]
# %%
file = local_files[0]
# read excel file
df = pd.read_excel(file, engine="openpyxl")
# %%
# drop columns with all NaN
df = df.dropna(how="all", axis=1)
# %%
# find the row index where row values contains 'Date'
date_row = df[df.apply(lambda row: "Date" in row.values, axis=1)].index[0]
# %%
# concate the date_row and date_row+1 and assign them as column names
df.columns = (
    df.iloc[date_row].fillna("") + " " + df.iloc[date_row + 1].fillna("")
).values.tolist()
# %%
# drop columns where 'merger' is present in the column name
df = df[[col for col in df.columns if "merger" not in col.lower()]]
# %%
# select rows from date_row+2 to the last row-2
df = df.iloc[date_row + 2 : -2]
# %%
col_names = {
    "Date ": "date",
    "M3 ": "m3",
    "1 Components (1.1.+1.2+1.3+1.4) 1.1 Currency with the Public": "currency_with_public",
    " 1.2 Demand Deposits with Banks": "demand_deposits_with_banks",
    " 1.3 Time Deposits with Banks": "time_deposits_with_banks",
    " 1.4 ‘Other’ Deposits with Reserve Bank": "other_deposits_with_rbi",
    "2 Sources (2.1+2.2+2.3+2.4-2.5) 2.1 Net Bank Credit to Government": "2.1",
    " 2.1.1 Reserve Bank": "reserve_bank_crd_to_gov_src",
    " 2.1.2 Other Banks": "other_banks_crd_to_gov_src",
    " 2.2 Bank Credit to Commercial Sector": "2.2",
    " 2.2.1 Reserve Bank": "reserve_bank_crd_to_comm_sctr_src",
    " 2.2.2 Other Banks": "other_banks_crd_to_comm_sctr_src",
    " 2.3 Net Foreign Exchange Assets of Banking Sector": "net_foreign_exchange_assets_src",
    " 2.4 Government's Currency Liabilities to the Public": "gov_currency_liab_to_public_src",
    " 2.5 Banking Sectors Net Non-Monetary Liabilities": "net_non_monetary_liab_bnk_sctr_src",
    " 2.5.1 Net Non-Monetary Liabilities of RBI": "net_non_monetary_liab_rbi_src",
    " ": "2.6",
}
df.rename(columns=col_names, inplace=True)
# %%
# drop column 2.1, 2.2 and 2.6
df = df.drop(columns=["2.1", "2.2", "2.6"])
# %%
df[1:].replace("-", None, inplace=True)
# convert all columns to numeric except date column
for c in df.columns[1:]:
    df[c] = pd.to_numeric(df[c])
    # decimal points to 2
    df[c] = df[c].round(2)
# %%
# convert date column to datetime
df["date"] = pd.to_datetime(df["date"]).dt.strftime("%d-%m-%Y")
# %%
date = str(df["date"].iloc[0])
# %%
# Save as csv file in processed folder
processed_csv_path = os.path.join(processed_path, "dbie_money_stocks.csv")
df.to_csv(processed_csv_path, index=False)
# Save as parquet file in processed folder
processed_parquet_path = os.path.join(
    processed_path, "dbie_money_stocks.parquet"
)
df.to_parquet(processed_parquet_path, index=False)

# %%
