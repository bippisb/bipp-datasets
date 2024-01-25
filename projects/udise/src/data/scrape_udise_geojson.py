import argparse
import json
import os.path
from pathlib import Path
from time import sleep

import requests
from requests.structures import CaseInsensitiveDict

# data folder
project_dir = str(Path(__file__).resolve().parents[2])
raw_folder = os.path.join(os.path.join(project_dir, "data"), "raw")
ext_folder = os.path.join(os.path.join(project_dir, "data"), "external")

file_name_format = "ud_dt_n_{udise_district_code}_page_{page_number_str}.geojson"


def get_data(udise_district_code, resultOffset):
    # sleep
    sleep(2)

    url = "https://geoportal.nic.in/nicgis/rest/services/SCHOOLGIS/Schooldata/MapServer/0/query?f=geojson&returnGeometry=true&outFields=*&where=ud_dt_n%3D'{udise_district_code}'&resultOffset={resultOffset}".format(
        udise_district_code=udise_district_code, resultOffset=resultOffset
    )
    print(url)
    headers = CaseInsensitiveDict()
    headers["Accept"] = "*/*"
    headers["Accept-Language"] = "en-GB,en-US;q=0.9,en;q=0.8"
    headers["Connection"] = "keep-alive"
    headers["Origin"] = "https://schoolgis.nic.in"
    headers["Referer"] = "https://schoolgis.nic.in/"
    headers["Sec-Fetch-Dest"] = "empty"
    headers["Sec-Fetch-Mode"] = "cors"
    headers["Sec-Fetch-Site"] = "cross-site"
    headers[
        "User-Agent"
    ] = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36"
    # headers["sec-ch-ua"] = "" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91""
    headers["sec-ch-ua-mobile"] = "?0"
    resp = requests.get(url, headers=headers)
    print(resp)
    return resp.json()


def write_data(data, file_name):
    f = open(file_name, "w")
    f.write(json.dumps(data))
    f.close()


def next_page_exists(data):
    if "exceededTransferLimit" in data:
        exceededTransferLimit = data["exceededTransferLimit"]
    else:
        exceededTransferLimit = False
    return exceededTransferLimit


def get_file_name(udise_district_code, page_number):
    page_number_str = "{:02d}".format(page_number)
    file_name = file_name_format.format(
        udise_district_code=udise_district_code, page_number_str=page_number_str
    )
    file_path = os.path.join(raw_folder, file_name)
    return file_path


def get_paged_data_and_save(udise_district_code, resultOffset, page_number):
    print(
        "udise_district_code, resultOffset, page_number",
        udise_district_code,
        resultOffset,
        page_number,
    )
    file_name = get_file_name(udise_district_code, page_number)
    data = get_data(udise_district_code, resultOffset)
    print(data)
    write_data(data, file_name)
    return next_page_exists(data)


def main(args):

    # Opening JSON file
    with open(os.path.join(ext_folder, "udise_district_code.json")) as json_file:
        udise_list = json.load(json_file)

    # Filter list if state_code is passed as an argument
    if args.statecode:
        udise_list = [
            state for state in udise_list if state["state_code"] == int(args.statecode)
        ]

    for state in udise_list:

        for udise_district_code in state["district_codes"]:
            resultOffset = 0
            page_number = 1
            # print(udise_district_code)
            file_name = get_file_name(udise_district_code, page_number)
            if not os.path.exists(file_name):
                next_page = get_paged_data_and_save(
                    udise_district_code, resultOffset, page_number
                )

                # print(next_page)

                while next_page:
                    page_number = page_number + 1
                    resultOffset = resultOffset + 1000
                    file_name = get_file_name(udise_district_code, page_number)
                    if not os.path.exists(file_name):
                        next_page = get_paged_data_and_save(
                            udise_district_code, resultOffset, page_number
                        )
                    else:
                        print(f"GeoJSON exists: {file_name}")
            else:
                print(f"GeoJSON exists: {file_name}")


if __name__ == "__main__":
    msg = "Description"
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--statecode",
        "-s",
        help="Input the start month in %m-%Y format",
        action="store",
        type=str,
        required=False,
    )
    args = parser.parse_args()

    main(args)
