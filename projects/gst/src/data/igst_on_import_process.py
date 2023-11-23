import pandas as pd
import numpy as np
from datetime import date

df = pd.read_excel('../data/raw/IGST-on-Import-2023-2024.xlsx')

# drop column where all values are NaN
df.dropna(axis=1, how='all', inplace=True)
# drop row where all values are NaN
df.dropna(axis=0, how='all', inplace=True)
df.reset_index(inplace=True, drop=True)

df = df.iloc[4:].copy()
df.reset_index(inplace=True, drop=True)

# Starting from the 3rd column (index 2) and taking 4 consecutive columns for each month
start_col = 0
month_dfs = []

# Loop through the columns to split them into separate DataFrames
while start_col < len(df.columns):
    # Extract columns for the current month (5 consecutive columns)
    month_data = df.iloc[:, start_col:start_col + 2]
    
    # Set column names for the month DataFrame
    month_data.columns = ["month", "igst"]
    
    # Add the month DataFrame to the list
    month_dfs.append(month_data)
    
    # Move to the next set of columns (next month)
    start_col += 2

merged_df = pd.concat(month_dfs)

# drop row where all values are NaN
merged_df.dropna(axis=0, how='all', inplace=True)
merged_df.reset_index(inplace=True, drop=True)

# drop rows where month == "Total"
merged_df = merged_df[merged_df["month"] != "TOTAL"]

merged_df.rename(columns={"month": "date"}, inplace=True)
merged_df['date'] = pd.to_datetime(merged_df['date'], format="%d-%m-%Y", errors='coerce')
merged_df['date'] = merged_df['date'].dt.strftime('%d-%m-%Y')

merged_df.to_csv('../data/interim/igst_on_import.csv', index=False)