import base64
import os
import random
import re
import time
from json.decoder import JSONDecodeError
from pathlib import Path

import pandas as pd
import requests

# data folder
project_dir = str(Path(__file__).resolve().parents[2])
interim_folder = os.path.join(os.path.join(project_dir, "data"), "interim")
raw_folder = os.path.join(os.path.join(project_dir, "data"), r"raw/report_cards")
os.makedirs(raw_folder, exist_ok=True)


def download_pdf(row):
    # print(row)
    state_folder = os.path.join(
        raw_folder,
        f"{str(row['state_lgd'])}_{re.sub('[^a-zA-Z]+', ' ', row['state_name'].strip().lower()).replace(' ', '_')}",
    )
    dist_folder = os.path.join(
        state_folder,
        f"{str(row['dist_lgd'])}_{re.sub('[^a-zA-Z]+', ' ', row['district_name'].strip().lower()).replace(' ', '_' )}",
    )
    vill_folder = os.path.join(
        dist_folder,
        re.sub("[^a-zA-Z]+", " ", row["village_name"].strip().lower()).replace(
            " ", "_"
        ),
    )
    # print(vill_folder)
    os.makedirs(vill_folder, exist_ok=True)

    file_name = f"{re.sub('[^a-zA-Z]+', ' ', row['schname'].strip().lower() ).replace(' ','_')}.pdf"
    file_path = os.path.join(vill_folder, file_name)

    if not os.path.exists(file_path):

        url = "https://src.udiseplus.gov.in/reportCardByApi/PdfReportUdiseCdApi"
        headers = dict()
        headers["Accept"] = "*/*"
        headers["Accept-Language"] = "en-GB,en-US;q=0.9,en;q=0.8"
        headers["Connection"] = "keep-alive"
        headers["Content-Type"] = "text/plain"
        headers["Content-Length"] = "62"
        headers["Host"] = "src.udiseplus.gov.in"
        headers["Origin"] = "https://schoolgis.nic.in"
        headers["Referer"] = "https://schoolgis.nic.in/"
        headers["Sec-Fetch-Dest"] = "empty"
        headers[
            "sec-ch-ua"
        ] = '"Google Chrome";v="107", "Chromium";v="107", "Not=A?Brand";v="24"'
        headers["Sec-Fetch-Mode"] = "cors"
        headers["Sec-Fetch-Site"] = "cross-site"
        headers[
            "User-Agent"
        ] = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36"
        # headers["sec-ch-ua"] = "" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91""
        headers["sec-ch-ua-mobile"] = "?0"

        data = {
            "udiseCode": row["udise_sch_code"],
            "clientId": "gis",
            "clientKey": "gis",
        }
        # print(data)
        resp = requests.post(url, headers=headers, json=data)

        print(resp.status_code)
        if resp.status_code == 200:
            try:
                resp_dict = resp.json()

                # print(resp_dict)
                if resp_dict["responseData"] is not None:
                    file_name = f"{re.sub('[^a-zA-Z]+', ' ', row['schname'].strip().lower() ).replace(' ','_')}.pdf"
                    with open(file_path, "wb") as f:
                        f.write(base64.b64decode(resp_dict["responseData"]))
                    print("File Downloaded: ", file_path)
                else:
                    print("No Response")
                    no_card_df = pd.DataFrame([row])
                    no_card_df.to_csv(
                        os.path.join(raw_folder, "no_report_cards.csv"),
                        mode="a",
                        index=False,
                        header=False,
                    )
            except JSONDecodeError as e:
                print("Empty Response", e)
                no_card_df = pd.DataFrame([row])
                no_card_df.to_csv(
                    os.path.join(raw_folder, "no_report_cards.csv"),
                    mode="a",
                    index=False,
                    header=False,
                )
        time.sleep(random.uniform(0.5, 3))
    else:
        print("PDF already exists: ", file_name)


def main():
    df_dtype = {
        "stname": str,
        "state_lgd": str,
        "dtname_1": str,
        "dist_lgd": str,
        "vilname": str,
        "schname": str,
        "schcd": str,
    }
    cols = [
        "stname",
        "state_lgd",
        "dtname_1",
        "dist_lgd",
        "vilname",
        "schname",
        "schcd",
    ]
    school_df = pd.read_csv(
        os.path.join(interim_folder, r"original/udise_schools.csv"),
        low_memory=False,
        dtype=df_dtype,
        usecols=cols,
    )
    new_col_names = {
        "stname": "state_name",
        "dtname_1": "district_name",
        "vilname": "village_name",
        "schcd": "udise_sch_code",
    }
    school_df = school_df.rename(columns=new_col_names)
    school_df = school_df.sort_values(
        by=["state_name", "district_name", "village_name"]
    )
    school_df = school_df.fillna("")
    rows = school_df.to_dict("records")

    if not os.path.exists(os.path.join(raw_folder, "no_report_cards.csv")):
        df = pd.DataFrame(columns=school_df.columns)
        df.to_csv(os.path.join(raw_folder, "no_report_cards.csv"), index=False)
        del df
    del school_df

    for row in rows:
        download_pdf(row)


if __name__ == "__main__":
    main()
