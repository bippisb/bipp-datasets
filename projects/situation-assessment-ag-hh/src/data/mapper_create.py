"""This script creates the mapper file with which each file will be converted into State, District, Social Group, Land holding size"""

from pathlib import Path

import pandas as pd

# defining paths
dir_path = Path.cwd()
raw_path = Path.joinpath(dir_path, "data", "raw")
interim_path = Path.joinpath(dir_path, "data", "interim")

agg_folder = interim_path.joinpath("agg_folder")
if not agg_folder.exists():
    agg_folder.mkdir(parents=True)


def level_path_creator(level):

    level_path = interim_path.joinpath(f"level{level}.csv")
    # print(level_path)

    return level_path


def agg_file_create():

    # The modus operandi is to extract information from Level 3 and Level 4 files to create a mapper file and
    # it can be used to create the aggregate files at the desired granularity (defined by the mapper file)

    # Extracting commonID, State, District and social group from level 1
    l3 = pd.read_csv(level_path_creator(3))

    social_group_df = l3[["common_id", "state", "district", "social_group_code"]]
    social_group_df = social_group_df.rename(
        columns={"social_group_code": "social_group"}
    )

    # extracting land owned information from Level 4
    l4 = pd.read_csv(level_path_creator(4))  # info on land held
    l4.drop(["land_agr_prod", "sr_no"], axis=1, inplace=True)

    l4_wide = l4[["common_id", "area_of_land", "categories_of_land"]].pivot(
        index="common_id", columns="categories_of_land", values="area_of_land"
    )

    # merging social_group_df and l4_wide to make the mapper file
    mapper_file = pd.merge(
        left=social_group_df,
        right=l4_wide,
        left_on="common_id",
        right_on="common_id",
        how="outer",
        validate="1:1",
    )

    print(mapper_file.columns)

    return


agg_file_create()
