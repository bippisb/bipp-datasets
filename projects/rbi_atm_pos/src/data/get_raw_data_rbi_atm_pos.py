import os
import time

# import datefinder
# import urllib.request
from pathlib import Path

import requests

# from datetime import datetime
from bs4 import BeautifulSoup
from lxml import html

project_dir = str(Path(__file__).resolve().parents[2])
parent_folder = project_dir + "/data/raw/"

url = "https://rbi.org.in/Scripts/ATMView.aspx"
response = requests.get(url)
soup = BeautifulSoup(response.text)

session = requests.Session()
curr_link = None
year_list = soup.findAll("div", {"class": "accordionButton year"})
hiddenvalues = soup.find_all("input", type="hidden")
# creating a dictionary to get the list of hidden values for all the
value_dict = {}
for each_hidden_value in hiddenvalues:
    name = each_hidden_value.get("name")
    value = each_hidden_value.get("value")
    value_dict[name] = value
if "__EVENTTARGET" in value_dict.keys():
    event_target = value_dict["__EVENTTARGET"]
else:
    event_target = ""

if "__EVENTARGUMENT" in value_dict.keys():
    event_argument = value_dict["__EVENTARGUMENT"]
else:
    event_argument = ""

if "__VIEWSTATE" in value_dict.keys():
    view_state = value_dict["__VIEWSTATE"]
else:
    view_state = ""

if "__VIEWSTATEGENERATOR" in value_dict.keys():
    view_state_gen = value_dict["__VIEWSTATEGENERATOR"]
else:
    view_state_gen = ""

if "__EVENTVALIDATION" in value_dict.keys():
    event_validation = value_dict["__EVENTVALIDATION"]
else:
    event_validation = ""


# iterating for all the years that are listed on the rbi website
for each_year in year_list:
    year = each_year.get_text()

    formdata = {
        "__EVENTTARGET": event_target,
        "__EVENTARGUMENT": event_argument,
        "__VIEWSTATE": view_state,
        "__VIEWSTATEGENERATOR": view_state_gen,
        "__EVENTVALIDATION": event_validation,
        "hdnYear": year,
        "hdnMonth": "0",
        "UsrFontCntr$txtSearch": "",
        "UsrFontCntr$btn": "",
    }
    response = session.post(url=url, data=formdata)
    time.sleep(5)
    tree = html.fromstring(response.content)
    xls_links = [
        a
        for a in tree.xpath('//table[@class="tablebg"]//td//a/@href')
        if a.endswith("XLSX") or a.endswith("XLS") or a.endswith("xls")
    ]
    session.close()
    if xls_links:
        print(year)
        print(xls_links)
    for link in xls_links:
        print(os.path.basename(link))
        local_excel_path = os.path.join(
            parent_folder,
            (os.path.join("rbi_atm_pos", f"{year}")),
        )
        Path(local_excel_path).mkdir(parents=True, exist_ok=True)
        local_excel_path = os.path.join(
            local_excel_path, f"rbi_atm_pos_{os.path.basename(link)}"
        )
        print(local_excel_path)
        file_name = local_excel_path.split("/")[-1]
        resp = requests.get(link)
        with open(local_excel_path, "wb") as output:
            output.write(resp.content)
