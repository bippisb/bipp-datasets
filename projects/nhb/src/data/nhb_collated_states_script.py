import os
from pathlib import Path

import numpy as np
import pandas as pd

project_dir = str(Path(__file__).resolve().parents[0])
Final = pd.DataFrame()
finaldata = []
for subdir, dirs, files in os.walk(project_dir):
    for file in files:
        if file.endswith(".csv"):
            df = pd.read_csv(
                os.path.join(subdir, file),
                header=[0, 1],
                encoding="ISO-8859-1",
                skiprows=10,
                skipfooter=2,
                index=False,
            )
            finaldata.append(df)

finaldata = pd.concat(finaldata)
finaldata.columns = [
    "Sr. No.",
    "project_code",
    "state",
    "year_of_subsidy_sanctioned",
    "district",
    "name_of_beneficiary",
    "project_address",
    "total_project_cost(in_lakhs)",
    "total_amount_sanctioned(in_lakhs)",
    "supported_by",
    "current_status_of_project",
    "component_category",
    "component",
    "number_of_unit(s)/chamber(s)",
    "capacity",
    "unit_of_capacity",
]
finaldata.drop(["Sr. No."], axis=1, inplace=True)

finaldata.insert(loc=0, column="Sr. No.", value=np.arange(1, len(finaldata) + 1))
finaldata.reset_index(drop=True, inplace=True)
finaldata.to_csv(project_dir + "/nhb_collated_states.csv")
