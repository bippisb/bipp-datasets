#%%
import pandas as pd
import numpy as np
from pathlib import Path
#%%
""" raw_folder may changes on the basis of data whether it is import or export
    Similarly output folder may changes on the basis of data whether it is import or export"""

# access all the files
folder_dir = Path.cwd()
raw_folder = folder_dir / "data" / "raw" / "export"
# %%
dfs = []
for file in raw_folder.iterdir():
    print(file)
    file_path = raw_folder / file
    df = pd.read_csv(file_path)
    df.rename(columns={'UNIT':'units', 
                       'PORT':'port', 
                       'QTY':'quantity', 
                       'COMMODITY':'commodity', 
                       'CTY':'country', 
                       'DVALUE':'dollars_value', 
                       'VALUE':'inr_value',
                       'HS_CODE':'pc_code'}, inplace=True)
    df = df[['month', 'port', 'country', 'hs_code','commodity', 'units', 'quantity', 'dollars_value', 'inr_value']]
    df['month'] = pd.to_datetime(df['month'])
    df['month'] = df['month'].dt.strftime('%d-%m-%Y')
    str_col = ['port', 'country', 'commodity', 'units']
    for col in str_col:
        df[col] = df[col].str.title().str.strip()
    dfs.append(df)
#%%
df = pd.concat(dfs)
df = df.reset_index(drop=True)
# %%
df.to_parquet("../data/interim/consolidated_export_port_level.parquet", index=False)
# %%
