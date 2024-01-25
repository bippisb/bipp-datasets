import json
import os.path
import re
from pathlib import Path

import pandas as pd
import requests

# data folder
project_dir = str(Path(__file__).resolve().parents[2])
raw_folder = os.path.join(os.path.join(project_dir, "data"), "raw")
data_folder = os.path.join(raw_folder, "schools_tabular")
os.makedirs(data_folder, exist_ok=True)

ext_folder = os.path.join(os.path.join(project_dir, "data"), "external")


def get_headers():

    headers = dict()
    headers["Accept"] = "*/*"
    headers["Accept-Language"] = "en-GB,en-US;q=0.9,en;q=0.8"
    headers["Connection"] = "keep-alive"
    headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"
    # headers["Content-Length"] = "62"
    # headers["Host"] = "src.udiseplus.gov.in"
    headers["Origin"] = "https://src.udiseplus.gov.in"
    headers["Referer"] = "https://src.udiseplus.gov.in/locateSchool/schoolSearch"
    headers["Sec-Fetch-Dest"] = "empty"
    headers[
        "sec-ch-ua"
    ] = '"Google Chrome";v="107", "Chromium";v="107", "Not=A?Brand";v="24"'
    headers["Sec-Fetch-Mode"] = "cors"
    headers["Sec-Fetch-Site"] = "same-origin"
    headers[
        "User-Agent"
    ] = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36"
    headers["sec-ch-ua-mobile"] = "?0"
    headers["sec-ch-ua-platform"] = "macOS"

    return headers


def get_school_list(state_id, district_id, block_id):
    headers = get_headers()
    payload = f"stateName={state_id}&districtName={district_id}&blockName={block_id}&villageId=&clusterId=&categoryName=0&managementName=0&Search=search"

    resp = requests.post(
        "https://src.udiseplus.gov.in/locateSchool/searchSchool",
        headers=headers,
        data=payload,
    )
    # print(resp.content)
    school_list = resp.json()
    if school_list["message"] is None:
        return school_list["list"]
    else:
        print("An error occured in School API")
        return []
    # return []


def main(state_list):
    # full_list = []
    for state in state_list:
        state_name_lower = re.sub(
            "[^a-zA-Z]+", " ", state["stateName"].strip().lower()
        ).replace(" ", "_")
        state_folder = os.path.join(data_folder, state_name_lower)
        os.makedirs(state_folder, exist_ok=True)

        for district in state["districts"]:
            dist_name_lower = re.sub(
                "[^a-zA-Z]+", " ", district["districtName"].strip().lower()
            ).replace(" ", "_")
            dist_folder = os.path.join(state_folder, dist_name_lower)
            os.makedirs(dist_folder, exist_ok=True)

            for block in district["blocks"]:
                blk_name_lower = f"block_{re.sub('[^a-zA-Z]+', ' ', block['eduBlockName'].strip().lower()).replace(' ','_')}.csv"
                blk_path = os.path.join(dist_folder, blk_name_lower)

                if not os.path.exists(blk_path):
                    schools = get_school_list(
                        state["stateId"], district["districtId"], block["eduBlockId"]
                    )
                    df = pd.DataFrame.from_dict(schools)
                    # print(df.head())
                    df.to_csv(blk_path, index=False)


if __name__ == "__main__":

    with open(os.path.join(raw_folder, "state_dist_block.json"), "r") as fin:
        state_list = json.load(fin)

    # state_list = [state_list[0]]

    main(state_list)
