import pandas as pd
import os

# Define the path to the folder containing the processed CSV files
processed_folder = "./raw/"

# Specify the path and name for the consolidated CSV file
consolidated_csv_path = "consolidated_data.csv"

# Create an empty list to store DataFrames from individual CSV files
dfs = []

# List all CSV files in the processed folder
csv_files = [file for file in os.listdir(processed_folder) if file.endswith(".csv")]

# Iterate through each CSV file and append its data to the list of DataFrames
for csv_filename in csv_files:
    csv_file_path = os.path.join(processed_folder, csv_filename)
    df = pd.read_csv(csv_file_path)
    dfs.append(df)

# Concatenate all DataFrames in the list into a single consolidated DataFrame
consolidated_df = pd.concat(dfs, ignore_index=True)

# Old Data 
df = pd.read_excel("./airport_freight.xlsx")

# Update Date format
df['year'].astype('str')
df['year'] = pd.to_datetime(df['year'], format='%Y-%d-%m')
df['year'] = df['year'].dt.strftime('%d-%m-%Y')
# Split the 'year' column into separate columns for day, month, and year
df[['day', 'month', 'year']] = df['year'].str.split('-', expand=True)
# Convert the day, month, and year columns to integers if needed
df['day'] = df['day'].astype(int)
df['month'] = df['month'].astype(int)
df['year'] = df['year'].astype(int)
# Proper format
df['date'] = df['month'].astype(str) + '-' + df['day'].astype(str) + '-' + df['year'].astype(str)
df.drop(['year', 'month', 'day','state_code'], axis=1, inplace=True)
df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')
df['date'] = df['date'].dt.strftime('%d-%m-%Y')

# create a dict for unique airport and state 
df_dict = df[['airport','state']].drop_duplicates().to_dict(orient='records')
# Convert the list of dictionaries into a dictionary
result_dict = {item['airport'].strip().lower(): item['state'] for item in df_dict}

# Use the map function to create the 'State' column
consolidated_df['state_name'] = consolidated_df['airport'].map(result_dict)
# Assign Maharashtra as the state name for Shirdi Airport
consolidated_df.loc[consolidated_df['airport'] == 'shirdi', 'state_name'] = 'Maharashtra'

# Convert the 'Date' column to datetime format with format='infer'
consolidated_df['date'] = pd.to_datetime(consolidated_df['date'], infer_datetime_format=True, errors='coerce')
# Format the 'Date' column to the desired format
consolidated_df['date'] = consolidated_df['date'].dt.strftime('%d-%m-%Y')
# Rename the 'State' column to 'state_name' in old data
df.rename(columns={'state':'state_name'}, inplace=True)

# append consolidated data to df
result = pd.concat([df, consolidated_df], ignore_index=True)

# Lowercase the 'airport' column
result['airport'] = result['airport'].str.lower().str.strip()

# Save the consolidated data to a CSV file in a different location
consolidated_df.to_csv(consolidated_csv_path, index=False)

print(f"Consolidated data saved to {consolidated_csv_path}")
