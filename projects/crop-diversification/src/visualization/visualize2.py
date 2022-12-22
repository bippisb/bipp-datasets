import os

import altair as alt
import pandas as pd
import streamlit as st

path_ag = "./data/interim/agcensus_isb/"
path_nc_var = "ag_census_2015_2016/non_crop_variables_selected.xlsx"
path_nc_ungr = "ag_census_2015_2016/combined6_mapped_nc_ungr_15_16.csv"
path_c_ungr = "ag_census_2015_2016/combined6_mapped_c_ungr_15_16.csv"
path_nc_ungr10 = "ag_Census_2010_2011/combined6_mapped_nc_ungr_10_11.csv"

states_six = [
    "punjab",
    "maharashtra",
    "jharkhand",
    "chhattisgarh",
    "odisha",
    "karnataka",
]
states_drop = ["All six states"] + states_six

df_nc_ungr_nc15 = pd.read_csv(os.path.abspath(path_ag + path_nc_ungr))
df_c_ungr_nc15 = pd.read_csv(os.path.abspath(path_ag + path_c_ungr))
df_ungr_nc10 = pd.read_csv(os.path.abspath(path_ag + path_nc_ungr10))

tehsil_url = "https://raw.githubusercontent.com/Shahbaz67/bipp_personal/main/Tehsil_2020_v2_topo.json"
data_geo_t = alt.topo_feature(tehsil_url, "Tehsil_2020_v2_new")

state_url = "https://raw.githubusercontent.com/Shahbaz67/bipp_personal/main/State_2020.json"
data_geo_s = alt.topo_feature(state_url, "State_2020")

st.set_page_config(
    "Agcensus Dashboard",
    layout="wide",
    page_icon="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTn7yzSFLaw-ohj9VePWzYRzS2tfNLJLBOzWw&usqp=CAU",
)
st.title("AgCensus Visualisation:")
st.sidebar.title("Filter data")
st.markdown(
    """
    <style>
    [data-testid="stSidebar"][aria-expanded="true"] > div:first-child {
        width: 250px;
    }
    """,
    unsafe_allow_html=True,
)

# select by social group (crop & non crop)
soc_gr = df_nc_ungr_nc15["soc_grp"].unique()
myorder_soc = [4, 0, 1, 3, 2]
soc_gr = soc_gr[myorder_soc]

# select by size class (crop & non crop)
size_class = df_nc_ungr_nc15["size_class"].unique()
myorder_size = [15, 2, 4, 7, 11, 14]
size_class = size_class[myorder_size]

# select parameter by parameter description (non crop only)
# df of selected variables for non crop
df_var_selected = pd.read_excel(os.path.abspath(path_ag + path_nc_var))
df_var_selected = df_var_selected.iloc[:, 0:2]
index = [10, 11, 12, 13, 18, 19, 20, 21, 35, 37, 48, 49, 50]

parameter = [df_var_selected["Variable Name"][i] for i in index]
description = [df_var_selected["Description"][i] for i in index]
param_dict = dict(zip(parameter, description))
param_dict_rev = dict(zip(param_dict.values(), param_dict.keys()))

colors = [
    "yelloworangered",
    "yelloworangebrown",
    "yellowgreen",
    "yellowgreenblue",
    "redpurple",
    "purplered",
    "purpleblue",
    "purplebluegreen",
    "greys",
    "blues",
    "goldred",
    "goldorange",
    "goldgreen",
]
color_dict = dict(zip(parameter, colors))

# select a parameter by parameter description (crop only)
parameter_crop = ["no_hold", "irr_ar", "unirr_ar", "total"]
description_crop = [
    "Total Holdings Number",
    "Irrigated Area",
    "Unirrigated Area",
    "Total Area",
]

param_dict_crop = dict(zip(parameter_crop, description_crop))
param_dict_rev_crop = dict(
    zip(param_dict_crop.values(), param_dict_crop.keys())
)
colors_crop = ["yelloworangered", "goldred", "goldorange", "goldgreen"]
color_dict_crop = dict(zip(parameter_crop, colors_crop))
crop_names = [
    "paddy(101)",
    "wheat(106)",
    "maize(104)",
    "tur (arhar)(202)",
    "onion(708)",
]

select_year, select_state, select_param = st.beta_columns([4, 3, 5])

# Filter options
select_year = select_year.multiselect(
    "Select year", ["2010-11", "2015-16"], default=["2010-11", "2015-16"]
)
select_state = select_state.selectbox("Select a State", states_drop)
select_crop_type = st.sidebar.radio("Crop Type", ["Non-Crop", "Crop"])
if select_crop_type == "Crop":
    select_crop_name = st.sidebar.selectbox("Crop Name", crop_names)
select_soc_grp = st.sidebar.radio("Pick a social group", soc_gr)
select_size_class = st.sidebar.radio("Pick a size class", size_class)


@st.cache
def load_data(data, state):
    return data[data["state"].isin([state])]


@st.cache
def filter_data(data):
    if select_crop_type == "Non-Crop":
        data = data[
            (data["soc_grp"] == select_soc_grp)
            & (data["size_class"] == select_size_class)
        ]
    else:
        data = data[
            (data["soc_grp"] == select_soc_grp)
            & (data["size_class"] == select_size_class)
            & (data["crop_name"] == select_crop_name)
        ]
    return data


# map generate function
@st.cache(allow_output_mutation=True)
def generate_map(df):
    if select_state == "All six states":
        # df_vis = load_data(df, states_six)
        df_vis = filter_data(df)
        # state
        state_layer = (
            alt.Chart(data_geo_s)
            .mark_geoshape(fillOpacity=0, stroke="white")
            .properties(height=700)
        )
        alt.data_transformers.disable_max_rows()

        # tehsil
        tehsil_layer = (
            alt.Chart(data_geo_t)
            .mark_geoshape(stroke="black", strokeWidth=1)
            .encode(
                color=alt.Color(
                    value,
                    legend=alt.Legend(
                        type="symbol",
                        tickCount=10,
                        symbolType="square",
                        labelOverlap=False,
                        symbolSize=500,
                        columnPadding=0,
                        rowPadding=0,
                        symbolStrokeWidth=0,
                    ),
                    type="quantitative",
                    scale=alt.Scale(scheme=color_scheme),
                    title=field[select_param],
                ),
                tooltip=["state:O", "tehsil:O", value],
            )
            .transform_lookup(
                lookup="properties.Sb_Dt_LGD",
                from_=alt.LookupData(
                    data=df_vis,
                    key="lgd_code",
                    fields=["state", "tehsil", field[select_param]],
                ),
            )
            .properties(height=700)
        )
        alt.data_transformers.disable_max_rows()

        # tooltip
        tooltip_layer = (
            alt.Chart(data_geo_t)
            .mark_geoshape(
                fillOpacity=0,
            )
            .encode(
                color=value,
                tooltip=["state:O", "tehsil:O", value],
            )
            .transform_lookup(
                lookup="properties.Sb_Dt_LGD",
                from_=alt.LookupData(
                    data=df_vis,
                    key="lgd_code",
                    fields=["state", "tehsil", field[select_param]],
                ),
            )
        )
        alt.data_transformers.disable_max_rows()
        map = tehsil_layer + state_layer + tooltip_layer
    else:
        df_vis = load_data(df, select_state)
        df_vis = filter_data(df_vis)
        map = (
            alt.Chart(data_geo_t)
            .mark_geoshape(stroke="black", strokeWidth=1)
            .encode(
                color=alt.Color(
                    value,
                    legend=alt.Legend(
                        type="symbol",
                        tickCount=10,
                        symbolType="square",
                        labelOverlap=False,
                        symbolSize=500,
                        columnPadding=0,
                        rowPadding=0,
                        symbolStrokeWidth=0,
                    ),
                    type="quantitative",
                    scale=alt.Scale(scheme=color_scheme),
                    title=field[select_param],
                ),
                tooltip=["state:O", "tehsil:O", value],
            )
            .transform_lookup(
                lookup="properties.Sb_Dt_LGD",
                from_=alt.LookupData(
                    data=df_vis,
                    key="lgd_code",
                    fields=["state", "tehsil", field[select_param]],
                ),
            )
            .properties(height=700)
        )
        alt.data_transformers.disable_max_rows()
    return map


# Dashboard setup
if select_crop_type == "Non-Crop":
    select_param = select_param.selectbox(
        "select a parameter for non-crop", description
    )
    field = param_dict_rev
    value = str(field[select_param]) + ":Q"
    color_scheme = color_dict[field[select_param]]
    if select_year == ["2010-11"]:
        map = generate_map(df_ungr_nc10)
    elif select_year == ["2015-16"]:
        map = generate_map(df_nc_ungr_nc15)
    else:
        col1, col2 = st.beta_columns(2)
        col1.write("2010-11")
        col2.write("2015-16")
        map1, map2 = st.beta_columns(2)
        map1 = generate_map(df_ungr_nc10)
        map2 = generate_map(df_nc_ungr_nc15)
        map = map1 | map2
else:
    select_param = select_param.selectbox(
        "select a parameter for crop", description_crop
    )
    field = param_dict_rev_crop
    value = str(field[select_param]) + ":Q"
    color_scheme = color_dict_crop[field[select_param]]
    if select_year == ["2010-11"]:
        st.text(
            "No data available for crop (2010-11). Deselect year 2010 or change the crop type to Non-Crop from the left pane."
        )
    elif select_year == ["2015-16"]:
        map = generate_map(df_c_ungr_nc15)
    else:
        st.text(
            "No data available for crop (2010-11). Deselect year 2010 or change the crop to Non-Crop type from the left pane."
        )
st.altair_chart(map, use_container_width=True)
