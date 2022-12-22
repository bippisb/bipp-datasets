import json
from pathlib import Path

import pandas as pd


def main():
    """
    This function aggregates all the processed data for the country and converts to a single file
    """

    # project_code = "PaFPS"
    # project_link = "PhyFinReport"  # unused
    project_name = "2_physical-and-financial-project-summary"

    project_dir = str(Path(__file__).resolve().parents[3])
    parent_folder_raw = project_dir + "/data/raw/" + project_name + "/"
    parent_folder_interim = project_dir + "/data/interim/" + project_name + "/"

    with open(parent_folder_raw + "scraped_dataset.json", "r") as f:
        data = json.loads(f.read())

    country_data = pd.DataFrame()

    saved_files = filter(lambda d: d["filename"] is not None, data)
    for file in saved_files:
        state = file["state_name"]
        dist = file["dist_name"]
        block = file["block_name"]
        year = file["year_dict"][file["year"]]
        batch = file["batch_name"]
        colab = file["colab_name"]

        file_path = (
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
        file_name = batch + "_" + colab + ".csv"
        data = pd.read_csv(file_path + file_name)
        # drop heading rows
        if country_data.shape != (0, 0):
            data = data.drop([0, 1], axis=0)

        # drop total row
        data = data.drop(data.index[-1], axis=0)
        country_data = pd.concat((country_data, data))

    country_data.to_csv(parent_folder_interim + "all_country_data.csv")


main()
