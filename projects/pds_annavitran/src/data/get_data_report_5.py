import pandas as pd
import numpy as np
from requests.utils import requote_uri
from lxml import html, etree
import requests, os
from urllib import request
from datetime import date
from requests import session
import urllib3
import csv

from IPython.core.display import display, HTML
display(HTML("<style>.container { width:85% !important; }</style>"))

headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36", "Accept":"text/html,application/xhtml+xml,application/xml; q=0.9,image/webp,image/apng,*/*;q=0.8"}

dir_path = os.getcwd()
data_path = os.path.join(dir_path, 'monthly')
raw_data = os.path.join(dir_path, 'raw_data')
report_5_path = os.path.join(raw_data, 'report_5')

codes_url = "https://raw.githubusercontent.com/gursharan-info/lgd-mappings/master/csv/pds_state_name_codes.json"
state_name_codes = requests.get(codes_url).json()
state_name_codes.append({"name": "LAKSHADWEEP", "id": 31})
state_name_codes.append({"name": "PUNJAB", "id": 3})
state_name_codes.append({'name': 'Lakshadweep', 'id': 31})
state_name_codes.append({'name': 'Punjab', 'id': 3})

start_date = '2017-01-01'
end_date = '2023-09-01'

dates_list = pd.date_range(start_date, end_date, freq='MS')

# Disable InsecureRequestWarning (not recommended for production)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# Create a session object
session = requests.Session()

total_data = []
col_names = ['dfso',
             'total_rice_allocated_unautomated','total_wheat_allocated_unautomated',
             'total_coarse_grain_allocated_unautomated','total_fortified_rice_allocated_unautomated',
             'total_qty_allocated_unautomated',
             'total_rice_allocated_automated','total_wheat_allocated_automated',
             'total_coarse_grain_allocated_automated','total_fortified_rice_allocated_automated',
             'total_qty_allocated_automated',
             'total_rice_qty_allocated','total_wheat_qty_allocated',
             'total_coarse_grain_qty_allocated','total_fortified_rice_qty_allocated',
             'total_qty_allocated',
             'total_rice_distributed_unautomated','total_wheat_distributed_unautomated',
             'total_coarse_grain_distributed_unautomated','total_fortified_rice_distributed_unautomated',
             'total_qty_distributed_unautomated','percent_qty_distributed_unautomated',
             'total_rice_distributed_automated','total_wheat_distributed_automated',
             'total_coarse_grain_distributed_automated','total_fortified_rice_distributed_automated',
             'total_qty_distributed_automated','percentage_qty_distributed_automated',
             'total_qty_distributed_epos']
for curr_date in dates_list:
    year_num = int(curr_date.strftime("%Y"))
    month_num = int(curr_date.strftime("%m"))
    for idx, state in enumerate(state_name_codes):
        # https://annavitran.nic.in/AVL/unautomatedStDetailMonthDFSOWiseRpt?m=2&y=2022&s=28&sn=Andhra%20Pradesh
        url = requote_uri(f"https://annavitran.nic.in/AVL/unautomatedStDetailMonthDFSOWiseRpt?m={month_num}&y={year_num}&s={str(state['id']).zfill(2)}&sn={state['name']}")
        response = session.get(url, headers=headers, verify=False)
        tree = html.fromstring(response.content)
        try:
            table_tree = tree.xpath("//table[@id='example']")
            if len(table_tree) > 0:
                table = table_tree[0]
                table_str = etree.tostring(table,method='html')
                df = pd.read_html(table_str)[0]
                df.columns = range(df.shape[1])
                df = df.replace("-", 0)
                df.drop(0, axis=1, inplace = True)
                df.drop(0, axis=0, inplace = True)
                df.columns = col_names
                df = df.head(-1)
                df.insert(0, 'state_name', state['name'])
                df.insert(0, 'state_code', state['id'])
                df.insert(0, 'month_year',  date(year_num,month_num,1).strftime("%b-%Y"))
                num_cols = col_names[1:]
                df[num_cols] = pd.to_numeric(df[num_cols].stack()).unstack()
                df.to_csv(f"{report_5_path}\pds_report_5_{state['name']}_{month_num}_{year_num}.csv", index=False)
                print(f'{state}-{month_num}-{year_num} scrapped')
                total_data.append(df)            
            else:
                print("No Table present for: ",state['name'], month_num, year_num)
        except Exception as e:
            raise ValueError(e)
session.close()

master_df = pd.concat(total_data).reset_index(drop=True)
master_df['state_dfso'] = master_df['state_name'].str.lower().str.strip() + master_df['dfso'].str.lower().str.strip()
master_df.rename(columns={'state_name':'state_name_data','state_code':'state_code_data'}, inplace=True)
master_df.to_csv('../data/raw/pds_table_5_data.csv', index=False)

data_subset = master_df[['state_name_data','dfso','state_dfso']].drop_duplicates()
data_subset = data_subset.dropna()

lgd_codes_file = "https://raw.githubusercontent.com/gursharan-info/lgd-mappings/master/csv/LGD_pds_monthly_05102021.csv"
dfso_mapping = pd.read_csv(lgd_codes_file)
dfso_mapping = dfso_mapping[~dfso_mapping['district_code'].isna()].reset_index(drop=True)
dfso_mapping['state_dfso'] = dfso_mapping['state_name_data'].str.lower().str.strip() + dfso_mapping['dfso'].str.lower().str.strip()
dfso_mapping = dfso_mapping[['state_dfso','district_code','district_name','state_code','state_name']].drop_duplicates(subset=['state_dfso']).reset_index(drop=True)

final_codes = data_subset.merge(dfso_mapping, on='state_dfso', how='left')
final_codes = final_codes[['state_dfso','state_name_data','state_code','district_name','district_code']]
final_codes = final_codes.drop_duplicates(subset='state_dfso')
merged_df_3 = master_df.merge(final_codes, on='state_dfso', how='left').fillna(np.nan)

merged_df = merged_df_3[ ['month_year','state_name_data_x','state_code','district_name','district_code',
                        'total_rice_allocated_unautomated', 'total_wheat_allocated_unautomated',
                        'total_coarse_grain_allocated_unautomated',
                        'total_fortified_rice_allocated_unautomated',
                        'total_qty_allocated_unautomated', 'total_rice_allocated_automated',
                        'total_wheat_allocated_automated',
                        'total_coarse_grain_allocated_automated',
                        'total_fortified_rice_allocated_automated',
                        'total_qty_allocated_automated', 'total_rice_qty_allocated',
                        'total_wheat_qty_allocated', 'total_coarse_grain_qty_allocated',
                        'total_fortified_rice_qty_allocated', 'total_qty_allocated',
                        'total_rice_distributed_unautomated',
                        'total_wheat_distributed_unautomated',
                        'total_coarse_grain_distributed_unautomated',
                        'total_fortified_rice_distributed_unautomated',
                        'total_qty_distributed_unautomated',
                        'percent_qty_distributed_unautomated',
                        'total_rice_distributed_automated', 'total_wheat_distributed_automated',
                        'total_coarse_grain_distributed_automated',
                        'total_fortified_rice_distributed_automated',
                        'total_qty_distributed_automated',
                        'percentage_qty_distributed_automated', 'total_qty_distributed_epos'] ].copy()
merged_df.rename(columns={'state_name_data_x':'state_name','month_year':'month'}, inplace=True)
merged_df['state_name'] = merged_df['state_name'].str.title().str.replace(r'\bAnd\b','and').str.replace('\s+', ' ', regex=True).str.strip()
merged_df['district_name'] = merged_df['district_name'].str.title().str.replace('\s+', ' ', regex=True).str.strip()

merged_df['district_code'] = merged_df['district_code'].apply(lambda x: str(x).zfill(3) if x else np.nan)
merged_df['state_code'] = merged_df['state_code'].apply(lambda x: str(x).zfill(2) if x else np.nan)

merged_df['month'] = pd.to_datetime(merged_df['month'], format='%b-%Y')
merged_df['month'] = merged_df['month'].dt.strftime('%d-%m-%Y')

df['state_name'] = df['state_name'].replace('Laksahdweep','Lakshadweep')
mask = df['state_name'] == 'Lakshadweep'
df.loc[mask, 'state_code'] = '31'
df.loc[mask, 'district_name'] = 'Lakshadweep District'
df.loc[mask, 'district_code'] = '553'
df.drop_duplicates(inplace=True)

merged_df.to_csv('../data/interim/pds_report_5_df.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)