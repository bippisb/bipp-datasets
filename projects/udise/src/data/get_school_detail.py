import collections
import json
import os.path
import re
from pathlib import Path

import pandas as pd
import requests
from lxml import etree, html

# data folder
project_dir = str(Path(__file__).resolve().parents[2])
raw_folder = os.path.join(os.path.join(project_dir, "data"), "raw")
interim_folder = os.path.join(os.path.join(project_dir, "data"), "interim")
data_folder = os.path.join(raw_folder, "schools_tabular")


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


def get_school_details(schoolId):
    headers = get_headers()
    payload = {"schoolIdforDashSearch": schoolId}
    url = "https://src.udiseplus.gov.in/searchSchool/getSchoolDetail"

    try:
        resp = requests.post(url, headers=headers, data=payload)
        # print(resp.content)
        content = html.fromstring(
            resp.text.replace("\t", "").replace("\n", "").replace("\r", "").strip()
        )

        data_dict = {}

        school_profile = content.xpath(
            ".//div[@class[contains(.,'card-header')]]/following-sibling::*[@class[contains(.,'card-block')]]"
        )[0]
        school_profile_rows = school_profile.xpath(
            ".//div[@class[contains(.,'row')]]//div[@class[contains(.,'row')]]"
        )
        # print(len(school_profile_rows))
        for row in school_profile_rows:
            name = row.xpath("./div[@class[contains(.,'col-5')]]/b/text()")[0]
            name = re.sub("[^a-zA-Z]+", " ", name.lower()).strip().replace(" ", "_")

            value = row.xpath("./div[@class[contains(.,'col-7')]]/span/text()")
            value = value[0] if len(value) > 0 else ""
            # print(name, value)

            data_dict[name] = value
        # print(data_dict)

        basic_detail_rows = content.xpath(
            '//*[@id="collapseOne201819"]//div[@class[contains(.,"card-block")]]/div[@class[contains(.,"row")]]/div[@class[contains(.,"col-")]]'
        )

        for brow in basic_detail_rows[-4:]:
            bname = brow.xpath(".//div[@class[contains(.,'col-5')]]/b/text()")[0]
            bname = re.sub("[^a-zA-Z]+", " ", bname.lower()).strip().replace(" ", "_")

            bvalue = brow.xpath(".//div[@class[contains(.,'col-7')]]/span/text()")
            bvalue = bvalue[0] if len(bvalue) > 0 else ""
            # print(bname, bvalue)
            data_dict[bname] = bvalue

        facilities_rows = content.xpath(
            '//*[@id="collapseOne"]//div[@class[contains(.,"card-block")]]/div[@class[contains(.,"row")]]/div[@class[contains(.,"col-md-")]]'
        )
        for frow in facilities_rows:
            fname = frow.xpath(".//div[@class[contains(.,'col-')]]/b/text()")[0]
            fname = re.sub("[^a-zA-Z]+", " ", bname.lower()).strip().replace(" ", "_")
            fvalue = frow.xpath(".//div[@class[contains(.,'col-')]]/span/text()")
            fvalue = fvalue[0] if len(fvalue) > 0 else ""
            # print(fname, fvalue)
            data_dict[fname] = fvalue

        room_rows = content.xpath(
            '//*[@id="collapseTwo"]//div[@class[contains(.,"card-block")]]/div[@class[contains(.,"row")]]/div[@class[contains(.,"col-md-")]]'
        )
        for rrow in room_rows:
            rname = rrow.xpath(".//div[@class[contains(.,'col-')]]/b/text()")[0]
            rname = re.sub("[^a-zA-Z]+", " ", rname.lower()).strip().replace(" ", "_")
            rvalue = rrow.xpath(".//div[@class[contains(.,'col-')]]/span/text()")
            rvalue = rvalue[0] if len(rvalue) > 0 else ""
            # print(rname, rvalue)
            data_dict[rname] = rvalue

        teacher_rows = content.xpath(
            '//*[@id="collapsethree"]//div[@class[contains(.,"card-block")]]//table[@class[contains(.,"table mt-3")]]//td//text()'
        )
        data_dict["total_teachers"] = teacher_rows[1]

        # print(data_dict)

        students_table = content.xpath(
            '(//*[@id="collapsethree"]//div[@class[contains(.,"card-block")]]//table[@class[contains(.,"table")]])[1]'
        )[0]
        students_df = pd.read_html(
            etree.tostring(students_table, method="html"), header=0
        )[0]
        students_df = students_df.drop(students_df.columns[0], axis=1).reset_index(
            drop=True
        )
        students_df.columns = [
            re.sub("[^a-zA-Z]+", " ", col).strip().lower().replace(" ", "_")
            + "_students"
            for col in students_df.columns
        ]
        students_dict = students_df.to_dict("records")[0]

        return data_dict | students_dict
    except Exception as e:
        print("No Results", e)
        return None


if __name__ == "__main__":

    # final_dict = get_details(4651992)
    # print(final_dict)
    with open(os.path.join(raw_folder, "state_dist_block.json"), "r") as fin:
        state_list = json.load(fin)

    for state in state_list:
        state_name_lower = re.sub(
            "[^a-zA-Z]+", " ", state["stateName"].strip().lower()
        ).replace(" ", "_")
        state_folder = os.path.join(data_folder, state_name_lower)

        for district in state["districts"]:
            dist_name_lower = re.sub(
                "[^a-zA-Z]+", " ", district["districtName"].strip().lower()
            ).replace(" ", "_")
            dist_folder = os.path.join(state_folder, dist_name_lower)
            os.makedirs(dist_folder, exist_ok=True)

            for block in district["blocks"]:
                blk_name_lower = f"block_{re.sub('[^a-zA-Z]+', ' ', block['eduBlockName'].strip().lower()).replace(' ','_')}.csv"
                blk_path = os.path.join(dist_folder, blk_name_lower)

                save_dist_path = os.path.join(
                    os.path.join(
                        os.path.join(interim_folder, "school_details"), state_name_lower
                    ),
                    dist_name_lower,
                )
                # os.makedirs(save_dist_path, exist_ok=True)

                save_path = os.path.join(save_dist_path, blk_name_lower)
                # print(save_path)
                if not os.path.exists(save_path):
                    os.makedirs(save_dist_path, exist_ok=True)
                    df = pd.read_csv(
                        blk_path, usecols=["schoolId", "schoolName", "schoolStatus"]
                    )
                    df = df[df["schoolStatus"] == 0].copy()
                    school_id_list = df.to_dict("records")

                    school_dict_list = []

                    for school in school_id_list:
                        print(school["schoolId"])
                        school_detail_dict = get_school_details(int(school["schoolId"]))

                        if school_detail_dict is not None:
                            school_detail_dict_ord = collections.OrderedDict(
                                school_detail_dict
                            )
                        else:
                            school_detail_dict_ord = collections.OrderedDict(
                                {
                                    "school_name": "",
                                    "udise_code": "",
                                    "state": "",
                                    "district": "",
                                    "block": "",
                                    "cluster": "",
                                    "village": "",
                                    "pincode": "",
                                    "school_category": "",
                                    "school_type": "",
                                    "class_from": "",
                                    "class_to": "",
                                    "state_management": "",
                                    "national_management": "",
                                    "status": "",
                                    "location": "",
                                    "aff_board_sec": "",
                                    "aff_board_h_sec": "",
                                    "year_of_establishment": "",
                                    "pre_primary": "",
                                    "class_rooms": "",
                                    "other_rooms": "",
                                    "total_teachers": "",
                                    "pre_primary_students": "",
                                    "i_students": "",
                                    "ii_students": "",
                                    "iii_students": "",
                                    "iv_students": "",
                                    "v_students": "",
                                    "vi_students": "",
                                    "vii_students": "",
                                    "viii_students": "",
                                    "ix_students": "",
                                    "x_students": "",
                                    "xi_students": "",
                                    "xii_students": "",
                                    "class_students": "",
                                    "class_withpre_primary_students": "",
                                }
                            )

                        school_detail_dict_ord.update(
                            {"raw_list_school_name": school["schoolName"]}
                        )
                        school_detail_dict_ord.move_to_end(
                            "raw_list_school_name", last=False
                        )

                        school_detail_dict_ord.update(
                            {"raw_list_school_id": school["schoolId"]}
                        )
                        school_detail_dict_ord.move_to_end(
                            "raw_list_school_id", last=False
                        )

                        # print(school_detail_dict_ord)
                        school_dict_list.append(school_detail_dict_ord)

                    block_df = pd.DataFrame(school_dict_list)
                    block_df.to_csv(save_path, index=False)

                    # print(block_df.head())
