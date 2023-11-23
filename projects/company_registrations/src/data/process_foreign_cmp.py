import os
import pandas as pd

for_column_mapping = {
    'state'                          : 'state_name', 
    'roc'                            : 'roc', 
    'cin'                            : 'fcin', 
    'country of incorporation'        : 'country_of_incorporation', 
    'date'                           : 'date_of_registration', 
    'fcrn'                            : 'fcin', 
    'company_status'                 : 'company_status', 
    'type of office'                 : 'office_type', 
    'registered office address'      : 'registered_office_address', 
    'activity code'                  : 'activity_code', 
    'activty description'            : 'activity_description', 
    'company_name'                   : 'company_name', 
    'month'                           : 'month', 
    'fcin'                           : 'fcin', 
    'activity'                        : 'activity_code', 
    'foreign company present address in india'  : 'registered_office_address', 
    'month_name'                      : 'month_name', 
    'company status'                  : 'company_status', 
    'foreign office address'            : 'foreign_office_address', 
    'email'                           : 'company_email_id', 
    'company name'                    : 'company_name', 
    'date of registration'            : 'date_of_registration', 
    'description'                     : 'description', 
    'address'                         : 'registered_office_address', 
    'activity_description'            : 'activity_description', 
    'office type'                     : 'office_type', 
    'date_of_registration'            : 'date_of_registration', 
    'month name'                      : 'month_name', 
    'type'                            : 'company_type', 
    'activity description'            : 'activity_description', 
    'registered_office_address'       : 'registered_office_address', 
    'status'                          : 'company_status', 
    'activity_code'                   : 'activity_code',
}


folder_path = './data/interim/'
processed_path = './data/processed/'

# List all files in the folder
files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]

# Filter files that start with 'ic'
ic_files = [f for f in files if f.startswith('foreign')]

data_lst = []
# Iterate through each file
for file_name in ic_files:
    file_path = os.path.join(folder_path, file_name)
    # Read the first few rows of the file to get column names
    try:
        df = pd.read_csv(file_path)
        df = df.rename(columns=for_column_mapping)
        
        # Merge the data into the consolidated DataFrame
        data_lst.append(df)
        print(f'file={file_name},   Total Columns={df.shape[1]},    columns={df.columns.tolist()}')

    except pd.errors.EmptyDataError:
        print(f"File: {file_name} is empty.")
    except Exception as e:
        print(f"Error reading file {file_name}: {str(e)}")

merged_data = pd.concat(data_lst)

# Remove duplicates
merged_data.drop_duplicates(inplace=True)
# Rearrange the columns
merged_data = merged_data[['month', 'state_name', 'fcin', 'company_name', 'date_of_registration', 'roc', 
                           'office_type', 'company_status', 'activity_code', 'activity_description', 
                           'description', 'country_of_incorporation', 'foreign_office_address',
                            'registered_office_address', 'company_email_id']]
# Save the merged data
merged_data.to_csv(os.path.join(processed_path, 'foreign_new.csv'), index=False)