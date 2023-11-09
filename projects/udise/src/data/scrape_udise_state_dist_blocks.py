import json
import os.path
from pathlib import Path

import requests

# data folder
project_dir = str(Path(__file__).resolve().parents[2])
raw_folder = os.path.join(os.path.join(project_dir, "data"), "raw")
data_folder = os.path.join(os.path.join(project_dir, "data"), "schools_tabular")
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


def get_state_list():
    state_headers = get_headers()
    resp = requests.get(
        "https://src.udiseplus.gov.in/locateSchool/state", headers=state_headers
    )
    state_list = resp.json()
    return state_list


def get_dist_list(state_id):
    dist_payload = f"stateId={state_id}"
    dist_headers = get_headers()
    dist_resp = requests.post(
        "https://src.udiseplus.gov.in/locateSchool/getDistrict",
        headers=dist_headers,
        data=dist_payload,
    )
    dist_list = dist_resp.json()
    return dist_list


def get_block_list(dist_id):
    block_headers = get_headers()
    block_resp = requests.get(
        f"https://src.udiseplus.gov.in/locateSchool/getBlock?districtId={dist_id}",
        headers=block_headers,
    )
    block_list = block_resp.json()
    return block_list


if __name__ == "__main__":
    # main(args)
    state_list = get_state_list()

    for state_idx, state in enumerate(state_list):

        dist_list = get_dist_list(state["stateId"])

        # print(dist_list)
        state_list[state_idx]["districts"] = dist_list

        for dist_idx, district in enumerate(dist_list):

            block_list = get_block_list(district["districtId"])
            # print(block_list)
            state_list[state_idx]["districts"][dist_idx]["blocks"] = block_list
    # print(state_list)

    with open(os.path.join(raw_folder, "state_dist_block.json"), "w") as fout:
        json.dump(state_list, fout, indent=4, sort_keys=False)
