import pathlib

import numpy as np
import pandas as pd

# Importing LGD mapping index

lgd = pd.read_csv("./data/external/ac_panchayat.csv", encoding="iso-8859-1")

# renaming the concerned columns
lgd = lgd.rename(columns={"stname": "state_name", "dtname": "district_name"})

# subsetting for necessary columns
lgd = lgd[["state_name", "district_name", "gp_code", "gp_name", "AC_NO", "AC_NAME"]]

# uppercasing all names
lgd["state_name"] = lgd["state_name"].str.upper()
lgd["district_name"] = lgd["district_name"].str.upper()
# lgd["block_name"] = lgd["block_name"].str.upper()
lgd["gp_name"] = lgd["gp_name"].str.upper()

# concatenating states, districts, blocks and panchayats

lgd["state_dist_panch"] = lgd["state_name"] + lgd["district_name"] + lgd["gp_name"]

# creating a cumulative count on key var to make unique key
lgd["gp_cum_count"] = lgd.groupby("state_dist_panch").cumcount()
lgd["gp_cum_count"] = lgd["gp_cum_count"].astype(str)

# replacing zeros with NA in gp_cum_count
conditions = [lgd["gp_cum_count"] == "0"]
options = [" "]
lgd["gp_cum_count"] = np.select(conditions, options, default=lgd["gp_cum_count"])

# creating a new village var with cum-count concatenated to it
lgd["gp_name_new"] = lgd["gp_name"] + lgd["gp_cum_count"].fillna("")

# concatenating states, districts, blocks and gp_name_new to avoid duplicate keys

lgd["state_dist_panch"] = lgd["state_name"] + lgd["district_name"] + lgd["gp_name_new"]

# removing gp_cum_count
lgd = lgd.drop("gp_cum_count", axis=1)


# removing duplicates and NA in panchayat names
# lgd=lgd.drop_duplicates(subset="state_dist_panch")
# lgd=lgd[~lgd['state_dist_panch'].isna()]

# defining a lsit to store not lgd mapped and LGD mapped state details

not_lgd_list = []
lgd_list = []

# Mapping Panchayats from block mapped files


def panchayat_lgd_mapper(path_name):
    print(
        "******************************************************LGD Mapping Initiating******************************************"
    )

    files = list(
        pathlib.Path(path_name).glob("./data/processed/block_lgd_mapped/*.csv")
    )

    # filtering for test states
    files_new = []
    for file in files:
        if file.stem in (["UTTAR PRADESH", "UTTARAKHAND", "GOA", "PUNJAB", "MANIPUR"]):
            files_new.append(file)

    pathlib.Path(str(path_name + "/data/processed/panchayat_lgd_mapped")).mkdir(
        parents=False, exist_ok=True
    )

    for file in files_new:

        # Filtering each state and its entries
        if file.stem == "ANDAMAN AND NICOBAR":
            # since Andaman is recorded as below in lgd state column, the code breaks.This line serves to override the error. "Don't fret. this has been taken care of!!!"
            lgd_state_filtered = lgd[lgd["state_name"] == "ANDAMAN AND NICOBAR ISLANDS"]
        else:
            lgd_state_filtered = lgd[lgd["state_name"] == file.stem]

        print("LGD file filtered for", file.stem)

        # reading in the state csv files
        df = pd.read_csv("./data/processed/block_lgd_mapped/" + file.name)
        print(file.stem, "has been loaded")

        # uppercasing the block names in state files
        df["block_name"] = df["block_name"].str.upper()
        df["state"] = df["state"].str.upper()
        df["district"] = df["district"].str.upper()
        df["panchayat_name"] = df["panchayat_name"].str.upper()

        # concatenating states, districts and blocks for states

        df["state_dist_panch"] = df["state"] + df["district"] + df["panchayat_name"]

        print("Merging the Mapper to data")
        # Merging lgd and state file
        df1 = pd.merge(
            df,
            lgd_state_filtered,
            how="outer",
            left_on="state_dist_panch",
            right_on="state_dist_panch",
            validate="m:1",
            indicator=True,
        )
        print("Filtering for Unmapped Observations")

        # filtering unmapped observations
        not_lgd_mapped = df1[(df1["_merge"] == "left_only")][
            [
                "state",
                "district",
                "block_name",
                "panchayat_name",
                "state_dist_panch",
            ]
        ]

        not_lgd_mapped = not_lgd_mapped.drop_duplicates(subset="state_dist_panch")

        not_lgd_list.append(not_lgd_mapped)

        # filtering merged rows
        df1 = df1[(df1["_merge"] == "both")]

        # renaming columns before sorting the necessary columns
        df1 = df1.rename(
            columns={
                "gp_code": "panchayat_LGD_code",
            }
        )

        # function to sort and select necessary columns for final data set
        def column_sort(data):
            # setting arrangement of rows
            columns = [
                "state",
                "state_LGD_code",
                "district",
                "district_LGD_code",
                "block_name",
                "block_LGD_code",
                "panchayat_name",
                "panchayat_LGD_code",
                "AC_NO",
                "AC_NAME",
                "work_code",
                "work_started_date",
                "work_physically_completed_date",
                "finished_when",
                "sanction_amount_in_lakh",
                "total_amount_paid_since_inception_in_lakh",
                "total_mandays",
                "no_of_units",
                "is_secure",
                "work_status",
                "master_work_category_name",
                "work_category_name",
                "work_start_fin_year",
            ]

            data = data[columns]
            return data

        df1 = column_sort(df1)

        # writing state file to lgd_mapped directory
        print("Exporting", file.stem, "as CSV to directory")
        df1.to_csv(
            str("./data/processed/panchayat_lgd_mapped/" + file.name), index=False
        )

    print("First merge unmap exported")
    not_lgd = pd.concat(not_lgd_list, axis=0)
    not_lgd.to_csv("data/interim/lgd_before_fuzzy_election.csv", index=False)

    print("Looping has ended")

    print(
        "***********************************************************************************************************************"
    )


panchayat_lgd_mapper(".")
