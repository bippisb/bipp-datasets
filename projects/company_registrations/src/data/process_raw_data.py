#%%
import pandas as pd
import os
import numpy as np
import openpyxl as xl

# Folder paths
dir_path = os.getcwd()
data_path = os.path.join(dir_path, 'data')
raw_path = os.path.join(data_path, 'raw')
interim_path = os.path.join(data_path, 'interim')
processed_path = os.path.join(data_path, 'processed')

# get list of files
file_list = os.listdir(raw_path)

ic_lst = []
llp_lst = []
foregin_lst = []
# iterate through each file
for file in file_list:
    date = file.split('.')[0].split('_')[2:]
    date_string = ' '.join(date)
    # Adjust the format as per your actual date string format
    date = pd.to_datetime(date_string, format='mixed')
    # Now, you can use strftime
    formatted_date = date.strftime("01-%m-%Y")
    file_path = os.path.join(raw_path, file)
    excel_file = pd.read_excel(file_path, sheet_name=None)
    wb = xl.load_workbook(file_path)
    sheet_names = wb.sheetnames

    ic_sheet_name = next((name for name in sheet_names if 'indian' in name.lower()), None)
    if ic_sheet_name:
        ic = excel_file[ic_sheet_name]
        if 'state' in [str(value).strip().lower() for value in ic.columns]:
            ic.columns = ic.columns.str.lower().str.strip()
            ic = ic.loc[:, "cin":]
        else:
            state_index = None
            for index, row in ic.iterrows():
                if 'state' in [str(value).strip().lower() for value in row.values]:
                    state_index = index
                    break
            if state_index is not None:
                ic.columns = ic.iloc[state_index].astype(str).str.lower().values
                ic = ic.iloc[state_index+1:]
                ic.columns = ic.columns.str.lower().str.strip()
                ic = ic.loc[:, "cin":]
        ic.insert(0, 'month', formatted_date)
        ic_lst.append(ic)
        print(f"file:{file},    sheet:{ic_sheet_name}")
        print(f"Rows:{ic.shape[0]},    Columns:{ic.shape[1]}")
    else:
        print('No Indian Company sheet')


    llp_sheet_name = next((name for name in sheet_names if 'llp' in name.lower()), None)
    if llp_sheet_name:
        llp = excel_file[llp_sheet_name]
        if 'state' in [str(value).strip().lower() for value in llp.columns]:
            llp.columns = llp.columns.str.lower().str.strip()
            llp = llp.loc[:, "llpin":]
        else:
            state_index = None
            for index, row in llp.iterrows():
                if 'state' in [str(value).strip().lower() for value in row.values]:
                    state_index = index
                    break
            if state_index is not None:
                llp.columns = llp.iloc[state_index].astype(str).str.lower().values
                llp = llp.iloc[state_index+1:]
                llp.columns = llp.columns.str.lower().str.strip()
                llp = llp.loc[:, "llpin":]
        llp.insert(0, 'month', formatted_date)
        llp_lst.append(llp)
        print(f"file:{file},    sheet:{llp_sheet_name}")
        print(f"Rows:{llp.shape[0]},    Columns:{llp.shape[1]}")
    else:
        print('No LLP sheet')

    
    foregin_sheet_name = next((name for name in sheet_names if 'foreign' in name.lower()), None)
    col_name = ['fcin','cin','fcrn']
    if foregin_sheet_name:
        foreign = excel_file[foregin_sheet_name]
        if 'state' in [str(value).strip().lower() for value in foreign.columns]:
            foreign.columns = foreign.columns.str.lower().str.strip()
            for f in col_name:
                if f in foreign.columns:
                    foreign = foreign.loc[:, f:]
        else:
            state_index = None
            for index, row in foreign.iterrows():
                if 'state' in [str(value).strip().lower() for value in row.values]:
                    state_index = index
                    break
            if state_index is not None:
                foreign.columns = foreign.iloc[state_index].astype(str).str.lower().str.strip().values
                for f in col_name:
                    if f in foreign.columns:
                        foreign = foreign.loc[:, f:]
                        foreign = foreign.iloc[state_index+1:]
                        break
        foreign.insert(0, 'month', formatted_date)
        foregin_lst.append(foreign)
        print(f"file:{file},    sheet:{foregin_sheet_name}")
        print(f"Rows:{foreign.shape[0]},    Columns:{foreign.shape[1]}")
    else:
        print(f'file:{file},    No foreign company sheet')

    
    ic.to_csv(os.path.join(interim_path, f'ic_{date_string}.csv'), index=False)
    llp.to_csv(os.path.join(interim_path, f'llp_{date_string}.csv'), index=False)
    foreign.to_csv(os.path.join(interim_path, f'foreign_{date_string}.csv'), index=False)
