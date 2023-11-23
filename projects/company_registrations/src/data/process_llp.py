import os
import pandas as pd

llp_column_mapping = {
    'state'                             : 'state_name', 
    'roc'                               : 'roc', 
    'total obligation of contribution'  : 'obligation_of_contribution', 
    'number of designated partners'     : 'no_of_designated_partners', 
    'founded'                           : 'date_of_registration', 
    'roc location'                      : 'roc', 
    'activity desc'                     : 'activity_description', 
    'registered office address'         : 'registered_office_address', 
    'no. of partners'                   : 'no_of_partners', 
    'industrial activity description'   : 'activity_description', 
    'activity code'                     : 'activity_code', 
    # 'main discription'                  : 'description', 
    'month'                             : 'month', 
    'llpin'                             : 'llpin', 
    'description activity'              : 'activity_description', 
    'limited liability partnership name'    : 'llp_name', 
    # 'industrial activity'               : 'activity_code', 
    'date of incorporation'             : 'date_of_registration', 
    'district'                          : 'district_name', 
    'email'                             : 'company_email_id', 
    'unnamed: 6'                        : 'unnamed_6', 
    # 'description'                       : 'description', 
    'address'                           : 'registered_office_address', 
    'llp status'                        : 'llp_status', 
    'number of'                         : 'no_of_designated_partners', 
    'llp name'                          : 'llp_name', 
    'description \n(5 digit discription)'   : 'activity_description', 
    'industrial activity code'          : 'activity_code', 
    'obligation of contribution'       : 'obligation_of_contribution', 
    'month name'                        : 'month_name', 
    'obligation of contribution(rs.)'   : 'obligation_of_contribution', 
    'registered_office_address'         : 'registered_office_address', 
    'activity description'              : 'activity_description', 
    'no. of designated partners'        : 'no_of_designated_partners', 
    'number of partners'               : 'no_of_partners', 
    'status'                            : 'llp_status',               
    'activity_code'                     : 'activity_code',
}


folder_path = './data/interim/'
processed_path = './data/processed/'

# List all files in the folder
files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]

# Filter files that start with 'ic'
ic_files = [f for f in files if f.startswith('llp')]

data_lst = []
# Iterate through each file
for file_name in ic_files:
    file_path = os.path.join(folder_path, file_name)
    # Read the first few rows of the file to get column names
    try:
        df = pd.read_csv(file_path)
        df = df.rename(columns=llp_column_mapping)
        if 'main discription' in df.columns:
            df.rename(columns={'main discription': 'description','description':'activity_description'}, inplace=True)
        else:
            df.rename(columns={'description':'description'}, inplace=True)

        # Check if 'industrial activity' is in the columns
        if 'industrial activity' in df.columns:
            # Assuming 'industrial activity' is the original column name
            original_column_name = 'industrial activity'
            # Check if 'activity_code' is already present in the columns
            if 'activity_code' in df.columns:
                if 'description' in df.columns:
                    df.rename(columns={original_column_name: 'activity_description'}, inplace=True)
                else:
                    df.rename(columns={original_column_name: 'description'}, inplace=True)
            else:
                df.rename(columns={original_column_name: 'activity_code'}, inplace=True)

        
        # Merge the data into the consolidated DataFrame
        data_lst.append(df)
        print(f'file={file_name},   Total Columns={df.shape[1]},    columns={df.columns.tolist()}')

    except pd.errors.EmptyDataError:
        print(f"File: {file_name} is empty.")
    except Exception as e:
        print(f"Error reading file {file_name}: {str(e)}")

merged_data = pd.concat(data_lst)

# Rearrange the columns
merged_data = merged_data[['month', 'state_name', 'district_name', 'llpin', 'llp_name', 'date_of_registration', 
       'roc', 'no_of_partners', 'no_of_designated_partners',
       'obligation_of_contribution', 'activity_code', 'activity_description','description',
       'registered_office_address', 'company_email_id', 'llp_status' ]]

# Remove duplicates
merged_data.drop_duplicates(inplace=True)

# Save the merged data
merged_data.to_csv(os.path.join(processed_path, 'llp_new.csv'), index=False)