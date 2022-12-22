# %%
# import necessary libraries
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

# %%
# project directory
project_dir = str(Path(__file__).resolve().parents[2])
parent_folder = project_dir + "/data/raw/"
# %%
# loading census shapefile
census = gpd.read_file(
    parent_folder + "/india_subdist_2011/SUBDISTRICT_11.shp"
)
# %%
# extracting sub_dist code from c_code11
census.columns = census.columns.str.lower()

census = census.drop(
    columns=[
        "tot_hh",
        "tot_pop",
        "m_pop",
        "f_pop",
        "tot_l6",
        "m_l6",
        "f_l6",
        "tot_sc",
        "m_sc",
        "f_sc",
        "tot_st",
        "m_st",
        "f_st",
        "tot_lit",
        "m_lit",
        "f_lit",
        "tot_illt",
        "m_illt",
        "f_illt",
        "tot_w",
        "m_w",
        "f_w",
        "r_tot_hh",
        "r_tot_pop",
        "r_m_pop",
        "r_f_pop",
        "r_tot_l6",
        "r_m_l6",
        "r_f_l6",
        "r_tot_sc",
        "r_m_sc",
        "r_f_sc",
        "r_tot_st",
        "r_m_st",
        "r_f_st",
        "r_tot_lit",
        "r_m_lit",
        "r_f_lit",
        "r_tot_illt",
        "r_m_illt",
        "r_f_illt",
        "r_tot_w",
        "r_m_w",
        "r_f_w",
        "u_tot_hh",
        "u_tot_pop",
        "u_m_pop",
        "u_f_pop",
        "u_tot_l6",
        "u_m_l6",
        "u_f_l6",
        "u_tot_sc",
        "u_m_sc",
        "u_f_sc",
        "u_tot_st",
        "u_m_st",
        "u_f_st",
        "u_tot_lit",
        "u_m_lit",
        "u_f_lit",
        "u_tot_illt",
        "u_m_illt",
        "u_f_illt",
        "u_tot_w",
        "u_m_w",
        "u_f_w",
    ],
    axis=1,
)
# %%
# reading the codebook
codebook = pd.read_excel(
    parent_folder + "/St_Dt_SbDt_Bk_Vllg_LGD_Codes.xlsx",
    sheet_name="Tehsil(Sub-Dist)",
    engine="openpyxl",
)

codebook2 = pd.read_excel(
    parent_folder + "/St_Dt_SbDt_Bk_Vllg_LGD_Codes.xlsx",
    sheet_name="Village",
    engine="openpyxl",
)
# %%
# manipulating census code
census["c_code11"] = census["c_code11"].astype("string")
census["state_code"] = census["c_code11"].str[:2]
census["district_code"] = census["c_code11"].str[2:5]
census["sub_dist_code"] = census["c_code11"].str[5:10]
census["index"] = census.index

# %%
census["sub_dist_code"] = (
    pd.to_numeric(census.sub_dist_code, errors="coerce")
    .fillna("0")
    .astype(np.int64)
)
codebook["Sb_Dt_Cs2011_code"] = (
    pd.to_numeric(codebook.Sb_Dt_Cs2011_code, errors="coerce")
    .fillna("0")
    .astype(np.int64)
)
codebook["State Name(In English)"] = codebook[
    "State Name(In English)"
].str.strip()
codebook["Subdistrict Name(In English)"] = codebook[
    "Subdistrict Name(In English)"
].str.strip()
codebook2.columns = [
    x.lower().strip().replace(" ", "_") for x in codebook2.columns
]
codebook2["state_name"] = codebook2["state_name"].str.strip()
codebook2["block_name"] = (
    codebook2["block_name"].fillna("0").astype("string").str.strip()
)
# %%
code_cs_list = codebook.Sb_Dt_Cs2011_code.unique().tolist()
census_geo = census[["index", "geometry"]]
census_df = pd.DataFrame(census.drop(columns="geometry", axis=1))
census_df["code_book_values"] = ""
# %%
# comparing census shapefile data with codebook
data_not_found = []

for i, row in census_df.iterrows():
    code = row["sub_dist_code"]
    sb_name = row["name"]
    for j, vals in codebook.iterrows():
        sub_dist_code = vals["Sb_Dt_Cs2011_code"]
        sub_dist_name = vals["Subdistrict Name(In English)"]
        value = vals.tolist()
        if code == sub_dist_code:
            row["code_book_values"] = value
            census_df.at[i, "code_book_values"] = value
        else:
            # check for block name in village sheet
            # for k, data in codebook2.iterrows():
            #     b_name = data["block_name"].lower()
            #     vill_data = data.tolist()
            #     if sb_name == b_name:
            #         census_df.at[i, 'code_book_values'] = data
            pass
    if row["code_book_values"] == "":
        print("Data Not Found")
        row["code_book_values"] = ["", "", "", "", "", "", "", "", ""]
        census_df.at[i, "code_book_values"] = [
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
        ]
        data_not_found.append(row.tolist())

    else:
        print("Match Found")
        print(row)


# %%
census_left = pd.DataFrame(data_not_found)
census_final = pd.merge(census_df, census_geo, on="index")

# census_final.loc[(census_final.code_book_values = ''), 'code_book_values'] = ['', '', '', '', '', '', '', '', '']

# census_final[census_final['code_book_values']== ''].fillna(np.nan)

census_final[
    [
        "St_LGD_code",
        "St_Cs2011_code",
        "State Name(In English)",
        "Dt_LGD_code",
        "Dt_Cs2011_code",
        "District Name(In English)",
        "Sb_Dt_LGD_code",
        "Sb_Dt_Cs2011_code",
        "Subdistrict Name(In English)",
    ]
] = census_final["code_book_values"].tolist()

census_final = census_final.drop("code_book_values", axis=1)

geo_census = gpd.GeoDataFrame(census_final, geometry="geometry")
geo_census.to_file(parent_folder + "sub_district_matched.shp")
# %%
census_left.columns = census_df.columns
# %%
census_final.to_excel(
    "sub_district_matched.xls",
    index=False,
)
census_left.to_excel("sub_district_unmatched.xls", index=False)
# %%
# logic for mapping looking for blocks and villages from codebook
# and there by getting the new sub-district name
census_left["name"] = census_left["name"].str.lower()
census_left["district"] = census_left["district"].str.lower()
census_left["state_ut"] = census_left["state_ut"].str.lower()

codebook2["state_name"] = codebook2["state_name"].str.lower()
codebook2["district_name"] = codebook2["district_name"].str.lower()
codebook2["subdistrict_name"] = codebook2["subdistrict_name"].str.lower()
codebook2["village_name"] = codebook2["village_name"].str.lower()
codebook2["block_name"] = codebook2["block_name"].str.lower()

# %%

# # modifying state_name, dist_name, subdistrict_name, village_name and block_name to lower case
# dist_list = census_left['district'].unique().tolist()
# df_list = []

# for dist in dist_list:

#     data_dist = census_left[census_left['district'] == dist]
#     lgd_dist = codebook2[codebook2['district_name'] == dist]
#     for i, row in data_dist.iterrows():
#         cen_sub_dist_name = row['name']
#         cen_dist_name = row['district']
#         cen_row = row.tolist()
#         for j, val in lgd_dist.iterrows():
#             lgd_dist_name = val['district_name']
#             lgd_block_name = val['block_name']
#             lgd_village_name = val['village_name']
#             lgd_row = val.tolist()
#             if cen_sub_dist_name == lgd_block_name:
#                 data_dist.at[i, "code_book_values"] = lgd_row
#                 df_list.append(cen_row + lgd_row)
#             elif cen_sub_dist_name == lgd_village_name:
#                 data_dist.at[i, "code_book_values"] = lgd_row
#                 df_list.append(cen_row + lgd_row)
#         print(row)


# %%
for i, row in census_left.iterrows():
    cen_sub_dist_name = row["name"]
    cen_dist_name = row["district"]
    for j, val in codebook2.iterrows():
        lgd_block_name = val["block_name"]
        lgd_dist_name = val["district_name"]
        lgd_village_name = val["village_name"]
        lgd_row = val.tolist()
        if cen_sub_dist_name == lgd_block_name:
            row["code_book_values"] = lgd_row
            census_left.at[i, "code_book_values"] = lgd_row
        elif cen_sub_dist_name == lgd_village_name:
            row["code_book_values"] = lgd_row
            census_left.at[i, "code_book_values"] = lgd_row
        else:
            print("Match is not found")
    if row["code_book_values"] == "":
        print("NOT FOUND")
    else:
        print("MATCH FOUND")
        print(row)

# %%
