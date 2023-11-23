#%%
# importing libraries
import os
import html5lib
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
#%%
headers = {
    "user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Mobile Safari/537.36"
}
with requests.Session() as s:
    url = "https://pmksy.gov.in/microirrigation/Financial_Report.aspx"

    # extracting out the HTML content from url and store it in a variable
    r = s.get(url, headers=headers)
    r = BeautifulSoup(r.content, "html5lib")

    # filtering out the states name from HTML content and store it in dictionary named state
    states_content = r.find(attrs={"name": "DdlState"})
    state = {}
    for option in states_content.find_all("option"):
        string = option.text.lower()
        string = " ".join(string.split())
        state[string] = option["value"]

    # filtering out the financial years from HTML content and store it in dictionary named year
    year_content = r.find(attrs={"name": "DdlYear"})
    year = {}
    for option in year_content.find_all("option"):
        string = option.text
        string = " ".join(string.split())
        year[string] = option["value"]

    # filtering out the financial months from HTML content and store it in dictionary named month
    month_content = r.find(attrs={"name": "DdlMonth"})
    month = {}
    for option in month_content.find_all("option"):
        string = option.text.lower()
        string = " ".join(string.split())
        month[string.split(",")[0]] = option["value"]
#%%
# function to convert the text into titled format
def titled(str4):
    str4=str4.split(" ")
    for idx,ele in enumerate(str4):
        if len(ele.split('.'))<=1:
            temp=""
            for i in range(len(ele)):
                if i==0 and ele[i]>='a' and ele[i]<='z':
                    temp+=ele[i].upper()
                elif i>0 and ele[i]>='A' and ele[i]<='Z':
                    temp+=ele[i].lower()
                else:
                    temp+=ele[i]
            str4[idx]=temp.strip()
    return " ".join(str4)


# function to store extracted contents into the pandas dataframe and store it in csv file
def frame_data(r, str1, str2, str3, path):
    # initialize an empty list
    data = []
    str3 = str3[0].upper() + str3[1:]

    if str3=='Jun':
        str3+='e'

    # iterate over all the rows of a table
    for row in r.find_all("tr"):
        temp = []
        for idx, item in enumerate(row.find_all("td")):
            if idx == 0:
                patt=re.compile(r'\b[a-zA-Z]')
                string=item.text.strip()
                matches=patt.finditer(string)

                for match in matches:
                    i=match.span()[0]
                    break
                temp.append(titled(string[i:]))

                temp.append(titled(str1))
                temp.append(str2)
                temp.append(str3)
            else:
                temp.append(item.text)
        
        data.append(temp)

    # specify the columns name
    columns = [
        "dist_name",
        "state_name",
        "fin_year",
        "fin_month",
        "fin_target_drip",
        "fin_target_sprinkler",
        "fin_total_target",
        "fin_achivement_drip",
        "fin_achivement_sprinkler",
        "fin_total_achievement",
        "percentage_achievement",
    ]

    # converting the stored data into pandas dataframe and store it in a csv file
    df = pd.DataFrame(data=data[3:], columns=columns)
    df.to_csv(path, index=False)


# function to extract HTML contents of a table from url
def extract_table(str1, str2, str3, path):
    # initialize the params dictionary which contains all the parameters while sending the request to a web server
    params = {"DdlState": "", "DdlYear": "", "DdlMonth": "", "DdlCropCategory": "H"}
    params["DdlState"], params["DdlYear"], params["DdlMonth"] = (
        state[str1],
        year[str2],
        month[str3],
    )
    global r

    url = "https://pmksy.gov.in/microirrigation/Financial_Report.aspx"
    r = s.get(url, headers=headers)
    r = BeautifulSoup(r.content, "html5lib")

    # store all the parameters into params dictionary
    for tag in r.find_all("input"):
        if tag.has_attr("name") and tag.has_attr("value"):
            params[tag["name"]] = tag["value"]

    # passing all the parameters to a web server and fetching out the HTML contents
    r = s.post(url, data=params, headers=headers)
    r = BeautifulSoup(r.content, "html5lib")
    frame_data(r, str1, str2, str3, path)


# function to iterate over all the states, financial years and financial months and store it at a given location as a csv file
def store_data(base_path):

    # iterate over all the states
    for st in state.keys():
        # we do not need to store the records of all india so here we are ignoring it
        if st == "all india":
            continue
        path1 = os.path.join(base_path, st)
        # if directory is not available then create it
        if not os.path.isdir(path1):
            os.mkdir(path1)

        # iterate over all the financial years
        y = [y for y in year.keys()]
        for yr in y[6:]:
            path2 = os.path.join(path1, yr)

            # if directory is not available then create it
            if not os.path.isdir(path2):
                os.mkdir(path2)

            # iterate over all the financial months
            for mn in month.keys():
                string = month[mn]

                if len(string) == 1:
                    string = "0" + string
                if string == "01" or string == "02" or string == "03":
                    string = string + "20" + yr.split("-")[1]
                else:
                    string += yr.split("-")[0]

                path3 = os.path.join(path2, string)
                path3 += ".csv"
                # if file has not been created yet then store its data at path3
                if not os.path.isfile(path3):
                    extract_table(st, yr, mn, path3)

# path where we need to store all the files and folders
base_path = "../data/raw"
store_data(base_path)
