import os
import re
import shutil
from pathlib import Path

import pandas as pd

project_dir = str(Path(__file__).resolve().parents[2])
interim_folder = os.path.join(os.path.join(project_dir, "data"), "interim")
raw_folder = os.path.join(os.path.join(project_dir, "data"), r"raw/report_cards")

df_dtype = {
    "stname": str,
    "state_lgd": str,
    "dtname_1": str,
    "dist_lgd": str,
    "vilname": str,
    "schname": str,
    "schcd": str,
}
cols = ["stname", "state_lgd", "dtname_1", "dist_lgd", "vilname", "schname", "schcd"]
school_df = pd.read_csv(
    os.path.join(interim_folder, r"original/udise_schools.csv"),
    low_memory=False,
    dtype=df_dtype,
    usecols=cols,
)
new_col_names = {
    "stname": "state_name",
    "dtname_1": "district_name",
    "vilname": "village_name",
    "schcd": "udise_sch_code",
}

school_df = school_df.rename(columns=new_col_names)
school_df = school_df.sort_values(by=["state_name", "district_name", "village_name"])
school_df = school_df.fillna("")
rows = school_df.to_dict("records")
# print(school_df.tail(500))

for row in rows[-100]:
    state_folder = os.path.join(
        raw_folder, row["state_name"].strip().lower().replace(" ", "_")
    )
    dist_folder = os.path.join(
        state_folder,
        re.sub("[^a-zA-Z]+", " ", row["district_name"].strip().lower()).replace(
            " ", "_"
        ),
    )
    vill_folder = os.path.join(
        dist_folder,
        re.sub("[^a-zA-Z]+", " ", row["village_name"].strip().lower()).replace(
            " ", "_"
        ),
    )
    print(vill_folder)

    new_state_folder = os.path.join(
        raw_folder,
        f"{str(row['state_lgd'])}_{re.sub('[^a-zA-Z]+', ' ', row['state_name'].strip().lower()).replace(' ', '_')}",
    )
    new_dist_folder = os.path.join(
        new_state_folder,
        f"{str(row['dist_lgd'])}_{re.sub('[^a-zA-Z]+', ' ', row['district_name'].strip().lower()).replace(' ', '_' )}",
    )
    new_vill_folder = os.path.join(
        new_dist_folder,
        re.sub("[^a-zA-Z]+", " ", row["village_name"].strip().lower()).replace(
            " ", "_"
        ),
    )

    print(new_vill_folder)

    try:
        allfiles = os.listdir(vill_folder)
    except FileNotFoundError as e:
        print(e)
        continue

    for f in allfiles:
        # print(f)
        os.makedirs(new_vill_folder, exist_ok=True)
        if not os.path.exists(os.path.join(new_vill_folder, f)):
            shutil.copy(os.path.join(vill_folder, f), os.path.join(new_vill_folder, f))
