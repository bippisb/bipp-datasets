import pandas as pd
import numpy as np
import os

# Create an empty list to store DataFrames
all_dataframes = []

# Directory containing the files
folder_path = '../data/raw/'

# List all files in the directory
file_list = os.listdir(folder_path)

# Filter files with names containing "Tax-collection"
tax_collection_files = [file for file in file_list if 'ewb-data' in file]

# Iterate through the filtered files
for file_name in tax_collection_files:
    # Construct the full file path
    file_path = os.path.join(folder_path, file_name)

    print(file_name)
    
    # Read the Excel file into a DataFrame
    df = pd.read_excel(file_path)
    
    # drop column where all values are NaN
    df.dropna(axis=1, how='all', inplace=True)

    # drop row where all values are NaN
    df.dropna(axis=0, how='all', inplace=True)
    df.reset_index(inplace=True)
    
    # Starting from the 3rd column (index 2) and taking 4 consecutive columns for each month
    initial_col = df[[df.columns[1], df.columns[2], df.columns[3], df.columns[4]]]
    start_col = 5
    month_dfs = []

    # Loop through the columns to split them into separate DataFrames
    while start_col < len(df.columns):
        # Extract columns for the current month (3 consecutive columns)
        month_data = df.iloc[:, start_col:start_col + 3]
        
        # Set column names for the month DataFrame
        month_data.columns = ["num_of_supplier","num_of_eway_bill","asset_value"]
        
        # concat the month Data and the initial column
        month_data = pd.concat([initial_col, month_data], axis=1)
        # Add the month DataFrame to the list
        month_dfs.append(month_data)
        
        # Move to the next set of columns (next month)
        start_col += 3
    
    merged_df = pd.concat(month_dfs)
    
    # Rename the columns
    merged_df.columns = ["state_code", "state_name", "year","month", "num_of_supplier","num_of_eway_bill","asset_value"]
    merged_df.dropna(subset=["state_code"],inplace=True)

    # create a new type_of_supplies column and assign values from num_of_supplier where asset_value is NaN
    merged_df["type_of_supplies"] = np.where(merged_df["asset_value"].isnull(), merged_df["num_of_supplier"], np.nan)
    # ffill in the "date" column
    merged_df["type_of_supplies"].ffill(inplace=True)
    # type_of_supplies column is in lower case
    merged_df["type_of_supplies"] = merged_df["type_of_supplies"].str.lower()

    # remove rows in which state_code is State Cd
    merged_df = merged_df[merged_df["state_code"] != "STATE CODE"]
    
    # create date column by concatenating month and year and day is 01
    merged_df["date"] = "01"+ "-" + merged_df["month"].astype(str) + "-" + merged_df["year"].astype(str)
    # change date format to dd-mm-yyyy format 
    merged_df["date"] = pd.to_datetime(merged_df["date"], format="%d-%m-%Y", errors='coerce')
    merged_df['date'] = merged_df['date'].dt.strftime('%d-%m-%Y')

    # drop month and year columns
    merged_df.drop(["month", "year"], axis=1, inplace=True)

    # rearrange columns
    merged_df = merged_df[["date", "state_code", "state_name", "type_of_supplies", "num_of_supplier", "num_of_eway_bill", "asset_value"]]
    # Append the processed DataFrame to the list
    all_dataframes.append(merged_df)

# Merge all DataFrames in the list
final_df = pd.concat(all_dataframes)

# Save the final DataFrame to a CSV file
final_df.to_csv('../data/interim/updated_eway_bill.csv', index=False)

# Optional: Print the first few rows of the final DataFrame
print(final_df.head())
