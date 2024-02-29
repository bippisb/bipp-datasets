# %%
import json
import os
from urllib.parse import urlencode

import requests
from IPython.core.display import HTML, display

display(HTML("<style>.container { width:90% !important; }</style>"))

base_url = "https://www.mca.gov.in/bin/dms/searchDocList"
dir_path = os.getcwd()
data_path = os.path.join(dir_path, "data")
raw_path = os.path.join(data_path, "raw")
# %%
# create list of scraping urls for 5 pages
scraping_urls = []
for i in range(1, 6):
    query_dict = {
        "page": [i],
        "perPage": ["20"],
        "sortField": ["Date"],
        "sortOrder": ["D"],
        "searchField": ["Title"],
        "dialog": [
            '{"folder":"403","language":"English","totalColumns":3,"columns":["Title","Month","Year of report"]}'
        ],
    }
    scraping_url = f"{base_url}?{urlencode(query_dict, doseq=True)}"
    scraping_urls.append(scraping_url)
# %%
# session = requests.Session()
# Get data from each scraping url
for scraping_url in scraping_urls:
    print(scraping_url)
    resp = requests.get(scraping_url)
    data_dict = json.loads(resp.content)
    doc_list = json.loads(data_dict["documentDetails"])
    # %%
    for doc in doc_list:
        curr_link = dict(
            url=f"https://www.mca.gov.in/bin/dms/getdocument?mds={doc['docID']}&type=open",
            docType=doc["docType"],
        )
        raw_file_loc = os.path.join(
            raw_path,
            f"mca_raw_{doc['column2']}_{doc['column3']}.{curr_link['docType']}",
        )
        print(raw_file_loc)
        resp_xls = requests.get(curr_link["url"])
        with open(raw_file_loc, "wb") as output:
            output.write(resp_xls.content)
        output.close()
        # print file size in bytes
        print(os.path.getsize(raw_file_loc))

# %%
