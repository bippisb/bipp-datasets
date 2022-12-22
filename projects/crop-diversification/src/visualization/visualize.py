import json
import os

import altair as alt
import geopandas as gpd
import pandas as pd
import streamlit as st

path_ag = "./data/interim/agcensus_isb/"
path_six = "ag_census_2015_2016/mapped6_nc_15_16.csv"
rel_path = os.path.abspath(path_ag + path_six)

states_six = [
    "punjab",
    "maharashtra",
    "jharkhand",
    "chhattisgarh",
    "odisha",
    "karnataka",
]

df_nc15 = pd.read_csv(
    rel_path, index_col=0
)  # ungrouped df for all states and mapped for six states
df_vis_six = df_nc15[df_nc15["state"].isin(states_six)]  # df for visualisation


def read_topojson(path):
    gdf = gpd.read_file(path, driver="TopoJSON")
    return gdf


path_tehsil_shp = "Tehsil_2020_v2_topo.json"
rel_path_tehsil_shp = os.path.abspath(path_ag + path_tehsil_shp)

json_file = read_topojson(rel_path_tehsil_shp)
json_file.rename(
    columns={
        "Sb_Dt_LGD": "lgd_code",
        "State_UT_N": "state",
        "Dist_Name": "district",
        "Sub_Dist_N": "tehsil",
    },
    inplace=True,
)


def pre_process(gdf, id):
    gdf["id"] = gdf[id]
    gdf.columns = gdf.columns.str.lower()
    gdf["state"] = gdf["state"].str.lower()
    gdf = gdf.applymap(lambda s: s.lower() if type(s) == str else s)
    return gdf


json_file = pre_process(json_file, "lgd_code")

json_file_six = json_file[
    json_file["state"].isin(states_six)
]  # json file for six states

path_state_shp = "State_2020.json"
rel_path_state_shp = os.path.abspath(path_ag + path_state_shp)

json_state = read_topojson(rel_path_state_shp)
json_state.rename(
    columns={"State_LGD": "state_lgd", "stname": "state"}, inplace=True
)

json_state = pre_process(json_state, "state_lgd")


def json_feature_list(gdf):
    gdf = gdf.to_json()  # string datatype
    gdf = json.loads(gdf)  # dict type
    gdf = alt.Data(
        values=gdf["features"]
    )  # json_features['features] is list type
    return gdf


states_drop = ["All six states"] + states_six
select = st.selectbox("Select a State", states_drop)


def state_mp(state, color_col, color_title, tooltip):
    # Add states layer
    data_geo_state = json_feature_list(json_state)

    outline = (
        alt.Chart(data_geo_state)
        .mark_geoshape(stroke="white", fillOpacity=0, strokeWidth=1.5)
        .properties(width=800, height=800)
    )
    # Add Base Layer
    gdf_merged = json_file.merge(
        df_vis_six, left_on="id", right_on="lgd_code", how="inner"
    )  # geodataframe
    data_geo_tehsil = json_feature_list(gdf_merged)

    base = (
        alt.Chart(data_geo_tehsil, title="Visualisation for six states")
        .mark_geoshape(stroke="white", strokeWidth=0.5)
        .encode(
            alt.Color(
                color_col,
                type="quantitative",
                scale=alt.Scale(scheme="yelloworangered"),
                title=color_title,
            ),
            tooltip=tooltip,
        )
        .properties(width=800, height=800)
    )
    base1 = (
        alt.Chart(data_geo_tehsil)
        .mark_geoshape(
            fillOpacity=0,
        )
        .encode(tooltip=tooltip)
    )
    return base + outline + base1


# def main():
fig = state_mp(
    state=states_six,
    color_col="properties.hold_no:Q",
    color_title="no. of holdings",
    tooltip=[
        "properties.state_x:N",
        "properties.district_x:N",
        "properties.hold_no:Q",
        "properties.hold_ar:Q",
    ],
)

st.title("AgCensus Visualisation:")
# # st.subheader("Analize data at various levels")
st.altair_chart(fig, use_container_width=True)

# st.sidebar.title("Agcensus variable analysis:")
# st.markdown("This dashboard analysizes change for any agcensus feature for the year 2010-11 and 2015-16")
# st.sidebar.markdown("This dashboard analysizes change for any agcensus feature for the year 2010-11 and 2015-16")
