import json
from pathlib import Path

import pandas as pd


def clear_df(df):
    """
    This function removes empty rows and columns from the raw dataframes
    and returns the new dataframe
    """
    return df.drop(["Unnamed: 0", "0"], axis=1).drop(0, axis=0)


def add_meta(df, type_, value):
    """
    This function adds a new attribute to "cleared" dataframe with "type_" as the
    type of metadata and "value" as the value for the metadata
    """
    new_col = (
        [None]
        + ["Metadata"]
        + [type_]
        + [value] * (df.shape[0] - 3)
        + [float("Nan")]
    )
    df[df.shape[1] + 1] = pd.Series(new_col)
    return df


def ensure_directory(file_path):
    """
    This function ensures the directory path provided in file_path
    exists, if not creates the path
    """

    path_parts = file_path.split("/")
    for i in range(1, len(path_parts) + 1):
        present_path = "/".join(path_parts[:i])
        Path(present_path).mkdir(exist_ok=True)


def main():
    """
    This function cleans all the raw data files and adds all attributes
    """

    # project_code = "PaFPS"
    # project_link = "PhyFinReport"  # unused
    project_name = "2_physical-and-financial-project-summary"

    project_dir = str(Path(__file__).resolve().parents[3])
    parent_folder_raw = project_dir + "/data/raw/" + project_name + "/"
    parent_folder_interim = project_dir + "/data/interim/" + project_name + "/"

    with open(parent_folder_raw + "scraped_dataset.json", "r") as f:
        data = json.loads(f.read())

    saved_files = filter(lambda d: d["filename"] is not None, data)
    count = 0
    for file in saved_files:
        state = file["state_name"]
        dist = file["dist_name"]
        block = file["block_name"]
        year = file["year_dict"][file["year"]]
        batch = file["batch_name"]
        colab = file["colab_name"]

        file_read_path = (
            parent_folder_raw
            + "/"
            + state
            + "/"
            + dist
            + "/"
            + block
            + "/"
            + year
            + "/"
        )
        file_name = batch + "_" + colab + ".csv"

        data = pd.read_csv(file_read_path + file_name)
        # clean data
        data = clear_df(data)
        # add metas
        data = add_meta(data, "Batch", batch)
        data = add_meta(data, "Collaboration", colab)
        data = add_meta(data, "Year", year)
        data = add_meta(data, "Block", block)
        data = add_meta(data, "District", dist)
        data = add_meta(data, "State", state)

        file_save_path = (
            parent_folder_interim
            + "/"
            + state
            + "/"
            + dist
            + "/"
            + block
            + "/"
            + year
            + "/"
        )
        ensure_directory(file_save_path)
        data.to_csv(file_save_path + file_name)
        count += 1

    print("total files changed:", count)


main()
