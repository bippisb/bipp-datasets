import pandas as pd
import numpy as np
from pathlib import Path
import os

raw_path = Path(__file__).parents[2].joinpath("data/raw/")
interim_path = Path(__file__).parents[2].joinpath("data/interim/")
processed_path = Path(__file__).parents[2].joinpath("data/processed/")
external_path = Path(__file__).parents[2].joinpath("data/external/")

codes = pd.read_csv(external_path.joinpath("state_district_codes.csv"))

# list all xlsx files in all subdirectories of raw_path
xlsx_files = []
for root, dirs, files in os.walk(raw_path):
    for file in files:
        if file.endswith(".xlsx"):
            # append the file path to the list
            xlsx_files.append(os.path.join(root, file))

cost_dfs = []
component_dfs = []

column_renames = {
        'Project Code':'project_code', 
        'State':'state_name', 
        'Year of Subsidy Sanctioned':'year_of_subsidy_sanct',
        'District':'district_name', 
        'Name of Beneficiary':'beneficiary_name', 
        'Project Address':'project_address',
        'Total Project Cost(In Lakhs)':'project_cost', 
        'Total Amount Sanctioned (In Lakhs)':'amount_sanct',
        'Supported By':'supported_by', 
        'Current Status of Project':'current_status', 
        'Component Category':'component_category',
        'Component':'component', 
        'No. of Unit(s)':'units', 
        'Capacity':'capacity',
        'Unit of Capacity':'capacity_unit', 
        'Agency':'agency'
}
 
for file in xlsx_files:
    print(f"\033[1;32mFile: {file.split('/')[-1]}\033[0m")
    df = pd.read_excel(file)
    df.rename(columns=column_renames, inplace=True)

    if "APEDA" in file:
        if 'component_category' not in df.columns:
            df['component_category'] = None
        if 'supported_by' not in df.columns:
            df['supported_by'] = None

        cost_dfs.append(df[['project_code', 'year_of_subsidy_sanct', 'state_name', 
       'district_name', 'agency', 'supported_by', 'beneficiary_name', 'project_address', 
       'current_status', 'project_cost', 'amount_sanct']])
        component_dfs.append(df[['project_code', 'year_of_subsidy_sanct', 'state_name', 
       'district_name', 'agency', 'supported_by', 'beneficiary_name', 'project_address', 
       'current_status', 'component_category', 'component', 'units', 'capacity', 'capacity_unit']])
    
    else :
        if 'component_category' not in df.columns:
            df['component_category'] = None
        if 'supported_by' not in df.columns:
            df['supported_by'] = None

        cost_df = df[['project_code', 'year_of_subsidy_sanct', 'state_name', 
       'district_name', 'agency', 'supported_by', 'beneficiary_name', 'project_address', 
       'current_status', 'project_cost', 'amount_sanct']]
        cost_dfs.append(cost_df)

        component_df = df[['project_code', 'year_of_subsidy_sanct', 'state_name', 
       'district_name', 'agency', 'supported_by', 'beneficiary_name', 'project_address', 
       'current_status', 'component_category', 'component', 'units', 'capacity', 'capacity_unit']]
        # forward filling the missing values
        component_df = component_df.fillna(method='ffill')
        component_dfs.append(component_df)

    print(f"\033[1;33mFile: {file} processed\033[0m")

# concatenate the dataframes
cost_consolidated = pd.concat(cost_dfs, ignore_index=True)

# Drop rows where Year of Subsidy Sanctioned is NaN
cost_consolidated = cost_consolidated.dropna(subset=['year_of_subsidy_sanct'])
cost_consolidated.replace(np.nan, None, inplace=True)

for col in cost_consolidated.columns:
    if cost_consolidated[col].dtype == 'float':
        cost_consolidated[col] = cost_consolidated[col].round(decimals=2)

component_consolidated = pd.concat(component_dfs, ignore_index=True)

component_consolidated.replace(np.nan, None, inplace=True)
component_consolidated['component'] = component_consolidated['component'].astype(str).str.title()
component_consolidated['component_category'] = component_consolidated['component_category'].astype(str).str.title()
component_consolidated['units'].replace("-", None, inplace=True)
component_consolidated['capacity_unit'].replace({ "NOS":"Nos",
                                                 "NOT AVAILABLE":"Not Available",
                                                 "NOT GIVEN":"Not Given",
                                                 "MT PER ANNUM":"MT/ANNUM"}, inplace=True)

for col in component_consolidated.columns:
    if component_consolidated[col].dtype == 'float':
        component_consolidated[col] = component_consolidated[col].round(decimals=2)

comp_rename = {
'Blastfreezer':'Blast Freezer',
'Ca Storage':'CA Storage',
'Ca Store':'CA Store',
'Ca/Ma':'CA/MA',
'Cold Room':'Cold Room',
'Cold Storage':'Cold Storage',
'Coldstore':'Cold Store',
'Collectioncentre/Bmc':'Collection Centre/BMC',
'Deepfreezer':'Deep Freezer',
'Iqf':'IQF',
'Iqf,Ripering':'IQF/Ripening',
'Milkprocessing':'Milk Processing',
'Mobilepre-Cooler':'Mobile Pre-Cooler',
'Phm':'PHM',
'Pre-Cooling':'Pre-Cooling Unit',
'Reefertransport':'Refrigerated Transport',
'Refer Van':'Refrigerated Van',
'Refrigerated Transport Vehicle':'Refrigerated Transport',
'Refrigerated Van':'Refrigerated Van',
}

component_consolidated['component'].replace(comp_rename, inplace=True)

# Assign component category 'Cold / CA Storage' where component  is allowable
component_consolidated.loc[component_consolidated['component'] == 'Blast Freezer', 'component_category'] = 'Cold / CA Storage'
component_consolidated.loc[component_consolidated['component'] == 'Cold Store', 'component_category'] = 'Cold / CA Storage'
component_consolidated.loc[component_consolidated['component'] == 'Deep Freezer', 'component_category'] = 'Cold / CA Storage'
component_consolidated.loc[component_consolidated['component'] == 'Others', 'component_category'] = 'Cold / CA Storage'
component_consolidated.loc[component_consolidated['component'] == 'Refrigerated Transport', 'component_category'] = 'Post Harvest Infrastructure'
component_consolidated.loc[component_consolidated['component'] == 'Sorting/Gradingline', 'component_category'] = 'Cold / CA Storage'
component_consolidated.loc[component_consolidated['component'] == 'CA/MA', 'component_category'] = 'Cold / CA Storage'
component_consolidated.loc[component_consolidated['component'] == 'Collection Centre/BMC', 'component_category'] = 'Cold / CA Storage'
component_consolidated.loc[component_consolidated['component'] == 'Milk Processing', 'component_category'] = 'Cold / CA Storage'
component_consolidated.loc[component_consolidated['component'] == 'Mobile Pre-Cooler', 'component_category'] = 'Cold / CA Storage'
component_consolidated.loc[component_consolidated['component'] == 'Pre-Cooling Unit', 'component_category'] = 'Cold / CA Storage'
component_consolidated.loc[component_consolidated['component'] == 'IQF/Ripening', 'component_category'] = 'Cold / CA Storage'

component_consolidated['component_category'].replace({ "Cold / Ca Storage":"Cold / CA Storage",}, inplace=True)
# convert units to numeric
component_consolidated['units'] = pd.to_numeric(component_consolidated['units'])
component_consolidated['capacity'] = component_consolidated['capacity'].round(decimals=2)

# save the dataframes to csv files in interim folder
cost_consolidated.to_csv(interim_path.joinpath("cost_consolidated.csv"), index=False)
component_consolidated.to_csv(interim_path.joinpath("component_consolidated.csv"), index=False)