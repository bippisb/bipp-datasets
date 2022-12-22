import os

import altair as alt
import pandas as pd
import streamlit as st

path_ag = "./data/interim/agcensus_isb/"
path_nc_var = "Ag_Census_2010_2011/non_crop_variables_selected.xlsx"
path_six = "Ag_Census_2010_2011/mapped_nc_10_11.csv"
path_ungr = "Ag_Census_2010_2011/ungrouped_mapped_nc_10_11.csv"

states_six = [
    "punjab",
    "maharashtra",
    "jharkhand",
    "chhattisgarh",
    "odisha",
    "karnataka",
]
states_drop = ["All six states"] + states_six

df_ungr_nc10 = pd.read_csv(os.path.abspath(path_ag + path_ungr))


tehsil_url = "https://raw.githubusercontent.com/Shahbaz67/bipp_personal/main/Tehsil_2020_v2_topo.json"
data_geo_t = alt.topo_feature(tehsil_url, "Tehsil_2020_v2_new")

state_url = "https://raw.githubusercontent.com/Shahbaz67/bipp_personal/main/State_2020.json"
data_geo_s = alt.topo_feature(state_url, "State_2020")

st.title("AgCensus Visualisation:")
st.sidebar.title("Filter data")


# select by social group
soc_gr_code = df_ungr_nc10["soc_grp"].unique()
soc_gr_code.sort()
social_group = [
    "Institutional",
    "Others",
    "Scheduled Caste",
    "Scheduled Tribe",
    "All Social Group",
]


# select by size class
size_class_code = df_ungr_nc10["size_class"].unique()
size_class_code.sort()
order = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0]
size_class_code = [size_class_code[i] for i in order]
size_class = [
    "All Classes",
    "Marginal",
    "Small",
    "Semi Medium",
    "Medium",
    "Large",
    "Below 0.5",
    "(0.5-1.0)",
    "(1.0-2.0)",
    "(2.0-3.0)",
    "(3.0-4.0)",
    "(4.0-5.0)",
    "(5.0-7.5)",
    "(7.5-10.0)",
    "(10.0-20.0)",
    "20 & Above",
]


def get_key_soc(val):
    for value in social_group:
        if val == value:
            return social_group.index(value)


def get_key_siz(val):
    for value in size_class:
        if val == value:
            return size_class.index(value)


# df of selected variables for non crop
df_var_selected = pd.read_excel(os.path.abspath(path_ag + path_nc_var))
df_var_selected = df_var_selected.iloc[:, 0:3]
parameter = list(df_var_selected["Variable name"])
description = list(
    df_var_selected["Variable description"]
    + df_var_selected["Unit of measurement"]
)
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


# Filter options
select_state = st.selectbox("Select a State", states_drop)
soc_gr = st.sidebar.radio("Select a social group", social_group)
select_soc_grp = soc_gr_code[get_key_soc(soc_gr)]
siz_clas = st.sidebar.radio("Select a size class", size_class)
select_size_class = size_class_code[get_key_siz(siz_clas)]


# select parameter by parameter description
select_param = st.selectbox("select a parameter", description)
value = str(param_dict_rev[select_param]) + ":Q"
color_scheme = color_dict[param_dict_rev[select_param]]
df = df_ungr_nc10
field = param_dict_rev


@st.cache
def load_data(data, state):
    return data[data["state"] == state]


@st.cache
def filter_data(data, soc, size):
    return data[(data["soc_grp"] == soc) & (data["size_class"] == size)]


if select_state == "All six states":
    df_vis_six = filter_data(df, select_soc_grp, select_size_class)
    # state
    state_layer = (
        alt.Chart(data_geo_s)
        .mark_geoshape(fillOpacity=0, stroke="white")
        .properties(width=800, height=800)
    )
    alt.data_transformers.disable_max_rows()

    # tehsil
    tehsil_layer = (
        alt.Chart(data_geo_t)
        .mark_geoshape(stroke="black", strokeWidth=1)
        .encode(
            color=alt.Color(
                value,
                type="quantitative",
                scale=alt.Scale(scheme=color_scheme),
                title=field[select_param],
            ),
            tooltip=["state_name:O", "tehsil_name:O", value],
        )
        .transform_lookup(
            lookup="properties.Sb_Dt_LGD",
            from_=alt.LookupData(
                data=df_vis_six,
                key="lgd_code",
                fields=["state_name", "tehsil_name", field[select_param]],
            ),
        )
        .properties(width=800, height=800)
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
            tooltip=["state_name:O", "tehsil_name:O", value],
        )
        .transform_lookup(
            lookup="properties.Sb_Dt_LGD",
            from_=alt.LookupData(
                data=df_vis_six,
                key="lgd_code",
                fields=["state_name", "tehsil_name", field[select_param]],
            ),
        )
    )
    alt.data_transformers.disable_max_rows()
    map = tehsil_layer + state_layer + tooltip_layer


else:
    df_vis_six = load_data(df, select_state)
    df_vis_six = filter_data(df_vis_six, select_soc_grp, select_size_class)
    map = (
        alt.Chart(data_geo_t)
        .mark_geoshape(stroke="black", strokeWidth=1)
        .encode(
            color=alt.Color(
                value,
                type="quantitative",
                scale=alt.Scale(scheme=color_scheme),
                title=field[select_param],
            ),
            tooltip=["state_name:O", "tehsil_name:O", value],
        )
        .transform_lookup(
            lookup="properties.Sb_Dt_LGD",
            from_=alt.LookupData(
                data=df_vis_six,
                key="lgd_code",
                fields=["state_name", "tehsil_name", field[select_param]],
            )
            # ).transform_filter(
            #     alt.FieldEqualPredicate(
            #         field="properties.State_UT_N", equal=select.upper()
            #     )
        )
        .properties(width=800, height=800)
    )
    alt.data_transformers.disable_max_rows()

st.altair_chart(map, use_container_width=True)
