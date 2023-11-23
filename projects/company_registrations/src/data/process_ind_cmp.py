import os
import pandas as pd

ic_column_mapping = {
    'state':                        'state_name', 
    'company class':                'company_class', 
    'number of members (applicable only in case of co. without share capital)':'num_of_members', 
    'roc':                          'roc', 
    'company_type':                 'company_type', 
    'cin':                          'company_identification_number', 
    'company email id':             'company_email_id', 
    'paidup_capital':               'paidup_capital', 
    'company_status':               'company_status', 
    'registered office address':    'registered_office_address', 
    'paid-up capital (in thousand rs.)':    'paidup_capital', 
    'activity code':                'activity_code', 
    'listed flag':                  'listed_flag', 
    'company_name':                 'company_name', 
    'authorized capital':           'authorized_capital', 
    'month':                        'month', 
    'paid capital':                 'paidup_capital', 
    'inc-29':                       'inc_29', 
    'company address':              'registered_office_address', 
    'activity':                     'activity_code', 
    'company type':                 'company_type', 
    'month_name':                   'month_name', 
    'company category':             'company_category', 
    'date of incorporation':        'date_of_registration', 
    'company status':               'company_status', 
    'company':                      'company_status', 
    'email':                        'company_email_id', 
    'sub category':                 'sub_category', 
    'company name':                 'company_name', 
    'date of registration':         'date_of_registration', 
    'class':                        'company_class', 
    'paidup capital':               'paidup_capital', 
    'authorized capital (in thousand rs.)':'authorized_capital', 
    'description':                  'description', 
    'category':                     'company_category', 
    'address':                      'registered_office_address', 
    'activity_description':         'activity_description', 
    'company sub-category':         'company_type', 
    'paidup':                       'paidup_capital', 
    'authorized':                   'authorized_capital', 
    'date_of_registration':         'date_of_registration', 
    'industrial activity code':     'activity_code', 
    'month name':                   'month_name', 
    'paid up':                      'paidup_capital', 
    'authorized_capital':           'authorized_capital', 
    'nature of company':            'company_nature', 
    'registered_office_address':    'registered_office_address', 
    'activity description':         'activity_description', 
    'industrial description':       'activity_description', 
    'activity_code':                'activity_code',
}


folder_path = './data/interim/'
processed_path = './data/processed/'

# List all files in the folder
files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]

# Filter files that start with 'ic'
ic_files = [f for f in files if f.startswith('ic')]

data_lst = []
# Iterate through each file
for file_name in ic_files:
    file_path = os.path.join(folder_path, file_name)
    # Read the first few rows of the file to get column names
    try:
        df = pd.read_csv(file_path)
        df = df.rename(columns=ic_column_mapping)

        # Merge the data into the consolidated DataFrame
        data_lst.append(df)

    except pd.errors.EmptyDataError:
        print(f"File: {file_name} is empty.")
    except Exception as e:
        print(f"Error reading file {file_name}: {str(e)}")

merged_data = pd.concat(data_lst)
# Drop duplicate rows
merged_data.drop_duplicates(inplace=True)
# Rearrange the columns
merged_data = merged_data[['month', 'state_name', 'company_identification_number', 'company_name',
       'date_of_registration',  'roc',
       'company_class', 'company_status', 'company_type', 'company_category',
       'activity_code', 'activity_description', 'registered_office_address', 'company_email_id',
       'authorized_capital', 'paidup_capital']]

# Save the merged data to a CSV file
merged_data.to_csv(os.path.join(processed_path, 'ic_new.csv'), index=False)
