import glob
import os
from pathlib import Path

import hjson
import pandas as pd

# data folder
project_dir = str(Path(__file__).resolve().parents[2])
raw_folder = os.path.join(os.path.join(project_dir, "data"), "raw")
interim_folder = os.path.join(os.path.join(project_dir, "data"), "interim")
geo_folder = os.path.join(interim_folder, "geo_level")
os.makedirs(geo_folder, exist_ok=True)


def process_file(file_name):
    print("Processing {file_name}".format(file_name=file_name))

    source_raw_data_file_path = os.path.join(raw_folder, file_name)
    file_obj = open(source_raw_data_file_path, "r")
    file_data = file_obj.read()
    file_obj.close()

    data = hjson.loads(file_data)
    # print(data)
    df_list = []
    for feature in data["features"]:
        row = feature["properties"]
        row["id"] = feature["id"]
        row["lon"] = feature["geometry"]["coordinates"][0]
        row["lat"] = feature["geometry"]["coordinates"][1]
        # print(row)
        # insert_array.append(row)
        df = pd.DataFrame([row.values()], columns=row.keys())
        df_list.append(df)
    if len(df_list) > 0:
        combined_df = pd.concat(df_list).reset_index(drop=True)
        return combined_df
    return None


def main():
    file_list = glob.glob(raw_folder + "/**/*.geojson", recursive=True)

    for file_name in file_list:

        output_filename = os.path.join(
            geo_folder, f"{os.path.basename(file_name).replace('.geojson','')}.csv"
        )
        if not os.path.exists(output_filename):
            file_df = process_file(file_name)
            # print(file_df)
            if file_df is not None:
                file_df.to_csv(
                    output_filename,
                    encoding="utf-8",
                    index=False,
                )
        else:
            print("CSV file exists")


if __name__ == "__main__":

    # Uncomment to convert geoJSON to individiual csv files
    # main()

    # Comment in case you are running main function. This is to combine all the csv files into one and clean it
    final_filename = os.path.join(interim_folder, "UDISE_schools.csv")
    csv_file_list = glob.glob(geo_folder + "/*.csv", recursive=False)

    concat_df = (
        pd.concat([pd.read_csv(f) for f in csv_file_list])
        .drop_duplicates()
        .reset_index(drop=True)
    )

    concat_df = concat_df[~concat_df["objectid"].isna()].copy().reset_index(drop=True)
    concat_df = concat_df[~concat_df["udise_vico"].isna()].copy().reset_index(drop=True)

    concat_df.to_csv(final_filename, index=False)

    final_df = concat_df.drop(
        columns=[
            "id",
            "objectid",
            "ud_st_n",
            "ud_dt_n",
            "lgd_dt_ud",
            "schcat",
            "schtype",
            "schmgt",
            "udise_stco",
            "stcode11",
            "dtcode11",
            "udise_dtco",
            "lat",
            "lon",
            "stname",
            "sdtcode11",
            "sdtname",
            "location",
            "dtname_1",
        ]
    ).copy()
    new_col_names = {
        "schcd": "udise_sch_code",
        "dtname": "district_name",
        "vilname": "village_name",
        "stname_1": "state_name",
        "udise_vico": "udise_village_code",
        "dist_lgd": "district_code",
        "state_lgd": "state_code",
    }
    final_df.rename(columns=new_col_names, inplace=True)
    final_df = final_df[
        [
            "state_name",
            "state_code",
            "district_name",
            "district_code",
            "village_name",
            "udise_village_code",
            "pincode",
            "udise_sch_code",
            "schname",
            "school_cat",
            "school_typ",
            "rururb",
            "management",
            "longitude",
            "latitude",
        ]
    ]
    final_df["state_code"] = final_df["state_code"].astype("int64")
    final_df["district_code"] = final_df["district_code"].astype("int64")
    final_df["udise_village_code"] = final_df["udise_village_code"].astype("int64")
    # final_df['pincode'] = final_df['pincode'].astype('int64')
    final_df["udise_sch_code"] = final_df["udise_sch_code"].astype(str)

    final_df.to_csv(
        os.path.join(interim_folder, "UDISE_schools_condensed.csv"), index=False
    )
