import os

import pandas as pd


def read_csv_excel(path):
    """
    This function takes the path of a folder and
    reads the csv and excel files inside it
    and then returns a concatenated dataframe
    """
    os.chdir(path)
    csv_excel = []
    for file in os.listdir():
        if file.endswith(".csv"):
            x = pd.read_csv(file, index_col=0)
            csv_excel.append(x)
        elif file.endswith(".xlsx"):
            y = pd.read_excel(file, index_col=0)
            csv_excel.append(y)
        else:
            print("extension type is not acceptible")
    comb = pd.concat(csv_excel, axis=0, ignore_index=True)
    return comb


# path = './data/interim/agcensus_isb/csv_excel/'
# df = read_csv_excel(path)
# print(df)

path_nc15 = (
    "./data/interim/agcensus_isb/ag_census_2015_2016/non_crop_2015_2016/"
)
combined_nc15 = read_csv_excel(path_nc15)


def lower(df):
    df.columns = df.columns.str.lower()
    df = df.applymap(lambda s: s.lower() if type(s) == str else s)
    return df


def pre_process(df):
    df = df.drop(["uqid", "size_class", "soc_grp"], axis=1)
    df[df.columns[4:]] = df[df.columns[4:]].fillna(0)
    df = df.groupby(
        ["state", "district", "tehsil", "lgd_code"],
        as_index=False,
        dropna=False,
    ).sum()
    return df


combined_nc15 = lower(combined_nc15)
combined_drop15 = pre_process(combined_nc15)

states = combined_nc15["state"].unique()
states_six = [
    "punjab",
    "maharashtra",
    "jharkhand",
    "chhattisgarh",
    "odisha",
    "karnataka",
]

# df = combined_drop.loc[combined_drop['state'].isin(states_six)]
# df = df.reset_index(drop=True)
# null_df = df[df['lgd_code'].isnull()]
# null_df = null_df[null_df.columns[0:4]]
# null_df.drop(['lgd_code'], axis=1, inplace=True)
# null_df = null_df.reset_index(drop=True)
# null_df['lgd_mapped'] = np.nan
# null_df['sb_dt_cdbook'] = np.nan
# null_df['block_cdbook'] = np.nan
# null_df = null_df[null_df.columns[[0,1,2,4,3,5]]]
# null_df.to_csv('/content/drive/MyDrive/Agri_ISB/Ag_Census_2015_2016/null_df_mapped_15_16.csv')
