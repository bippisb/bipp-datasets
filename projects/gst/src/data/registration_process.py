import pandas as pd
import numpy as np
from datetime import date

# Read the Excel file
df = pd.read_excel('../data/raw/REGISTRATION.xlsx')

# drop last column
df.drop(df.columns[-1], axis=1, inplace=True)

# rename columns
df.columns = ["state_code", "state_name", "normal_tapayers","composition_taxpayers", "input_service_distributor",
              "casual_taxpayers","tax_collector_at_source","tax_deductor_at_source","non_residential_taxpayers",
              "oidar","uin_holders","total"]

# drop row where state_code is NaN
df.dropna(subset=["state_code"], inplace=True)
# remove first row
df = df[1:]

# create column date and assign the date of today in dd-mm-yyyy format
df['date'] = date.today()
df['date'] = pd.to_datetime(df['date'], format="%d-%m-%Y", errors='coerce')
df['date'] = df['date'].dt.strftime('%d-%m-%Y')

# Save the final DataFrame to a CSV file
df.to_csv('../data/interim/registrations.csv', index=False)