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
tax_collection_files = [file for file in file_list if 'GSTR-3B' in file]

# Iterate through the filtered files
for file_name in tax_collection_files:
    # Construct the full file path
    file_path = os.path.join(folder_path, file_name)

    print(file_name)
    
    # Read the Excel file into a DataFrame
    df = pd.read_excel(file_path)
    
    # Fill NaN values in row 5 with the preceding non-NaN value (forward fill)
    df.iloc[5].fillna(method='ffill', inplace=True)
    
    df = df.iloc[5:45].copy()
    df.reset_index(inplace=True)
    
    # Starting from the 3rd column (index 2) and taking 4 consecutive columns for each month
    initial_col = df[[df.columns[1], df.columns[2]]]
    start_col = 3
    month_dfs = []
    
    # Loop through the columns to split them into separate DataFrames
    while start_col < len(df.columns):
        # Extract columns for the current month (5 consecutive columns)
        month_data = df.iloc[:, start_col:start_col + 5]
        
        # Set column names for the month DataFrame
        month_data.columns = ["eligible_tax_payer", "filled_by_due_date", "filled_after_due_date", "total_return_filled","filling_percentage"]
        
        # Concatenate the month Data and the initial column
        month_data = pd.concat([initial_col, month_data], axis=1)
        # Add the month DataFrame to the list
        month_dfs.append(month_data)
        
        # Move to the next set of columns (next month)
        start_col += 5
    
    merged_df = pd.concat(month_dfs)
    
    # Rename the columns
    merged_df.columns = ["state_code", "state_name","eligible_tax_payer", "filled_by_due_date", "filled_after_due_date", "total_return_filled","filling_percentage"]
    
    # Create a new "date" column and assign values from "cgst" where "state_code" is "State Cd" else NaN
    merged_df["date"] = np.where(merged_df["state_code"] == "STATE CD", merged_df["eligible_tax_payer"], np.nan)
    
    # Forward fill in the "date" column
    merged_df["date"].ffill(inplace=True)
    
    # Remove rows with NaN and "State Cd" values in state_code
    merged_df.dropna(inplace=True)
    merged_df = merged_df[merged_df["state_code"] != "STATE CD"]
    
    # Append the processed DataFrame to the list
    all_dataframes.append(merged_df)

# Merge all DataFrames in the list
final_df = pd.concat(all_dataframes)

# Save the final DataFrame to a CSV file
final_df.to_csv('../data/interim/merged_gstr_3b.csv', index=False)

# Optional: Print the first few rows of the final DataFrame
print(final_df.head())
