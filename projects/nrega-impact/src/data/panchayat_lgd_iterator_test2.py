import pathlib

import numpy as np
import pandas as pd
from fuzzywuzzy import process

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
        if file.stem in ("UTTAR PRADESH"):
            files_new.append(file)

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
        print("past here!!!!")
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
        print("past here 2 !!!!")

        # tabs=df1['_merge'].value_counts()

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

        print("First merge unmap exported")

        not_lgd_mapped = not_lgd_mapped.drop_duplicates(subset="state_dist_panch")

        not_lgd_mapped.to_csv("data/interim/lgd_before_fuzzy_UP_.csv", index=False)

        # Fuzzywuzzy for mapping
        print("*******Initiating Fuzzy Mapping for ", file.stem, "********")

        # creating a list of matches for unmerged state_dist_block names
        result = [
            process.extractOne(i, lgd_state_filtered["state_dist_panch"])
            for i in not_lgd_mapped["state_dist_panch"]
        ]

        # converting and editing the fuzzy matches to a dataframe
        result = pd.DataFrame(result, columns=["match", "score", "id"])
        result.drop("id", axis=1, inplace=True)

        # creating a proxy dataframe with names of unmerged original names of state_dist_panch
        not_lgd_proxy_df = (
            pd.DataFrame(not_lgd_mapped["state_dist_panch"], index=None)
            .reset_index()
            .drop("index", axis=1)
        )

        # creating a dataframe for fuzzy mapper
        mapper_df = pd.concat(
            [not_lgd_proxy_df, result],
            axis=1,
            ignore_index=True,
            names=["original", "match", "score"],
        )
        mapper_df = mapper_df[mapper_df[2] >= 90]

        # creating a dictionary for fuzzy mapper
        mapper_dict = dict(zip(mapper_df[0], mapper_df[1]))

        # applying the mapper dictionary on the original processed state file to correct for fuzzy matched names
        df["state_dist_panch"] = df["state_dist_panch"].replace(mapper_dict)

        print(
            "*******Fuzzy Mapping for",
            file.stem,
            "has ended. Proceeding for second round of Data Merge*******",
        )

        # Second round of data merge with update fuzzy matched names along with exact original names in the state file
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

        df1["_merge"].value_counts()

        # filtering fuzzy unmapped observations
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

        # filtering fuzzy mapped observations
        lgd_mapped = df1[(df1["_merge"] == "both")][
            [
                "state",
                "district",
                "block_name",
                "panchayat_name",
                "state_name",
                "district_name",
                "gp_name",
                "gp_code",
                "AC_NAME",
                "AC_NO",
                "state_dist_panch",
            ]
        ]

        lgd_mapped = lgd_mapped.drop_duplicates(subset="state_dist_panch")

        lgd_list.append(lgd_mapped)

    # converting the list into a final dataframe
    not_lgd = pd.concat(not_lgd_list, axis=0)
    lgd_matched = pd.concat(lgd_list, axis=0)

    print("Exporting not_lgd and LGD matched as CSV file to the Interim sub directory")
    not_lgd.to_csv("data/interim/not_panch_lgd_after_fuzzy_UP_vill.csv", index=False)
    lgd_matched.to_csv("data/interim/panch_lgd_fuzzy_matched_UP_vill.csv", index=False)

    print("Looping has ended")

    print(
        "***********************************************************************************************************************"
    )


panchayat_lgd_mapper(".")
