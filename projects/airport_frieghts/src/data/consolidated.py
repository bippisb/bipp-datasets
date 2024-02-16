#%%
import pandas as pd
import os

# Define the path to the folder containing the processed CSV files
base_dir = os.path.dirname(os.path.dirname(os.getcwd()))
interim_folder = os.path.join(base_dir, "data", "interim")
# Specify the path and name for the consolidated CSV file
processed_folder = os.path.join(base_dir, "data", "processed")
external_folder = os.path.join(base_dir, "data", "external")
consolidated_csv_path = os.path.join(processed_folder, "consolidated_data.csv")
#%%
# Create an empty list to store DataFrames from individual CSV files
dfs = []

# List all CSV files in the processed folder
csv_files = [file for file in os.listdir(interim_folder) if file.endswith(".csv")]

# Iterate through each CSV file and append its data to the list of DataFrames
for csv_filename in csv_files:
    csv_file_path = os.path.join(interim_folder, csv_filename)
    df = pd.read_csv(csv_file_path)
    dfs.append(df)
#%%
# Concatenate all DataFrames in the list into a single consolidated DataFrame
consolidated_df = pd.concat(dfs, ignore_index=True)
consolidated_df['airport'] = consolidated_df['airport'].str.title().str.strip()
#%%
# Old Data 
df = pd.read_csv(os.path.join(external_folder, "airport_freight.csv"))
#%%
# create a dict for unique airport and state 
df_dict = df[['airport','state_name', 'state_code']].drop_duplicates().reset_index(drop=True)

#%%
# Create state_name column in consolidated_df based on airport
consolidated_df = pd.merge(consolidated_df, df_dict, on='airport', how='left')

#%%
# Convert the 'Date' column to datetime format with format='infer'
consolidated_df['date'] = pd.to_datetime(consolidated_df['date'], infer_datetime_format=True, errors='coerce')
# Format the 'Date' column to the desired format
consolidated_df['date'] = consolidated_df['date'].dt.strftime('%d-%m-%Y')
#%%
# append consolidated data to df
result = pd.concat([df, consolidated_df], ignore_index=True)
#%%
# convert freight to numeric
result['freight'] = result['freight'].apply(lambda x: int(str(x).replace(',', '')))

#%%
# state-code should be 2 characters 
result['state_code'] = result['state_code'].apply(lambda x: str(x).zfill(2))

#%%
# Reaaranging the columns
result = result[['date', 'state_name', 'state_code','airport','freight']]
#%%

result['date'] = pd.to_datetime(result['date'], format='%d-%m-%Y')


# Save the consolidated data to a CSV file in a different location
result.to_csv(consolidated_csv_path, index=False)

print(f"Consolidated data saved to {consolidated_csv_path}")

# %%
