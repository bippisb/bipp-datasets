import pandas as pd
import csv
import numpy as np
from lxml import html, etree
import requests, os
from bs4 import BeautifulSoup
from requests import session
import urllib3
from IPython.core.display import display, HTML
display(HTML("<style>.container { width:85% !important; }</style>"))

dir_path = os.getcwd()
data_path = os.path.join(dir_path, 'monthly')
raw_data = os.path.join(dir_path, 'raw_data')
report_8_path = os.path.join(raw_data, 'report_8')

headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36", "Accept":"text/html,application/xhtml+xml,application/xml; q=0.9,image/webp,image/apng,*/*;q=0.8"}
base_url = "https://annavitran.nic.in/AVL/reports"
response = requests.get(base_url)
if response.status_code == 200:
    soup = BeautifulSoup(response.text, 'html.parser')
    select_element = soup.find('select', id='selectedyear')
    if select_element:
        options = select_element.find_all('option')
        years = []
        for option in options:
            option_text = option.text
            option_value = option['value']
            years.append(option_value)
        print(years)
    else:
        print("Select element not found with the specified XPath.")
else:
    print(f"Failed to retrieve the page. Status code: {response.status_code}")

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

session = requests.Session()
total_data = []
col_names = ['state_name',
             'no_of_ration_cards', 
             'no_trans_inter_district_aay',
             'no_trans_inter_district_phh',
             'total_inter_trans',
             'no_trans_intra_district_aay',
             'no_trans_intra_district_phh',
             'total_intra_trans',
             'total_transaction',
             'qty_distributed_inter_district_aay',
             'qty_distributed_inter_district_phh',
             'total_inter_qty',
             'qty_distributed_intra_district_aay',
             'qty_distributed_intra_district_phh',
             'total_intra_qty',
             'total_qty' ]

for year in years[1:]:
    url = f"https://annavitran.nic.in/AVL/annavitranPortableiiRptFi?year={year}"
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
            df.insert(0, 'year',  year)
            num_cols = col_names[1:]
            df[num_cols] = pd.to_numeric(df[num_cols].stack()).unstack()
            df.to_csv(f"{report_8_path}\pds_report_8_{year}.csv", index=False)
            print(f'{year} scrapped')
            total_data.append(df)           
        else:
                print("No Table present for: ", year)
    except Exception as e:
        raise ValueError(e)

master_df = pd.concat(total_data).reset_index(drop=True)
master_df['year'] = pd.to_datetime(master_df['year'], format= '%Y', errors='coerce')
master_df['year'] = master_df['year'].dt.strftime('%d-%m-%Y')
master_df.to_csv('../data/raw/pds_table_8_data.csv', index=False)

codes_url = "https://raw.githubusercontent.com/gursharan-info/lgd-mappings/master/csv/pds_state_name_codes.json"
state_name_codes = requests.get(codes_url).json()
state_name_codes.append({"name": "LAKSHADWEEP", "id": 31})
state_name_codes.append({"name": "PUNJAB", "id": 3})
state_name_to_code = {entry['name']: str(entry['id']).zfill(2) for entry in state_name_codes}

master_df['state_name'] = master_df['state_name'].str.upper()
master_df['state_code'] = master_df['state_name'].map(state_name_to_code)
master_df.loc[master_df['state_name'] == 'DADRA & NAGAR HAVELI', 'state_code'] = 38

master_df = master_df[['year', 'state_name','state_code', 'no_of_ration_cards',
       'no_trans_inter_district_aay', 'no_trans_inter_district_phh',
       'total_inter_trans', 'no_trans_intra_district_aay',
       'no_trans_intra_district_phh', 'total_intra_trans', 'total_transaction',
       'qty_distributed_inter_district_aay',
       'qty_distributed_inter_district_phh', 'total_inter_qty',
       'qty_distributed_intra_district_aay',
       'qty_distributed_intra_district_phh', 'total_intra_qty', 'total_qty',
       ]]

master_df.to_csv("../data/interim/pds_report_8.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)