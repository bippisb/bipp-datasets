#%%
import pandas as pd
import numpy as np
from pathlib import Path
import os

interim_path = Path(__file__).parents[2].joinpath("data/interim/")
processed_path = Path(__file__).parents[2].joinpath("data/processed/")
external_path = Path(__file__).parents[2].joinpath("data/external/")
#%%
cost_df = pd.read_csv(interim_path.joinpath("cost_consolidated.csv"))
component_df = pd.read_csv(interim_path.joinpath("component_consolidated.csv"))

mapped_df = pd.read_csv(external_path.joinpath("cost_lgd_mapping.csv"))
mapped_df['state_lgd_code'] = mapped_df['state_lgd_code'].astype(str).str.zfill(2)
mapped_df['district_lgd_code'] = mapped_df['district_lgd_code'].astype(str).str.zfill(3)
#%%
mapped_cost = cost_df.merge(mapped_df, on=['state_name', 'district_name'], how='left')
# Drop state_name and district_name
mapped_cost = mapped_cost.drop(['state_name', 'district_name'], axis=1)
mapped_cost.rename(columns={'lgd_mapped_state_name': 'state_name', 
                            'state_lgd_code': 'state_code',
                            'lgd_mapped_district_name': 'district_name',
                            'district_lgd_code': 'district_code'}, inplace=True)
mapped_cost = mapped_cost[['project_code', 'year_of_subsidy_sanct','state_name', 'state_code', 
                           'district_name', 'district_code', 'agency', 'supported_by', 'beneficiary_name', 
                           'project_address', 'current_status', 'project_cost', 'amount_sanct' ]]
mapped_cost['state_name'] = mapped_cost['state_name'].str.title()
mapped_cost['district_name'] = mapped_cost['district_name'].str.title()
mapped_cost.replace(np.nan, None, inplace=True)

#%%
mapped_component = component_df.merge(mapped_df, on=['state_name', 'district_name'], how='left')
# Drop state_name and district_name
mapped_component = mapped_component.drop(['state_name', 'district_name'], axis=1)
mapped_component.rename(columns={'lgd_mapped_state_name': 'state_name', 
                            'state_lgd_code': 'state_code',
                            'lgd_mapped_district_name': 'district_name',
                            'district_lgd_code': 'district_code'}, inplace=True)
#%%
mapped_component = mapped_component[['project_code', 'year_of_subsidy_sanct','state_name', 'state_code', 
                           'district_name', 'district_code', 'agency', 'supported_by', 'beneficiary_name', 
                           'project_address', 'current_status','component_category', 'component', 
                           'units', 'capacity', 'capacity_unit' ]]
mapped_component['state_name'] = mapped_component['state_name'].str.title()
mapped_component['district_name'] = mapped_component['district_name'].str.title()
mapped_component.replace(np.nan, None, inplace=True)
#%%
mapped_cost.to_csv(processed_path.joinpath("cost_consolidated_mapped.csv"), index=False)
mapped_cost.to_parquet(processed_path.joinpath("cost_consolidated_mapped.parquet"), index=False)
mapped_component.to_csv(processed_path.joinpath("component_consolidated_mapped.csv"), index=False)
mapped_component.to_parquet(processed_path.joinpath("component_consolidated_mapped.parquet"), index=False)
# %%
