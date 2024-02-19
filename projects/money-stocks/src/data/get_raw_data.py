# import libraries
from bs4 import BeautifulSoup
from datetime import datetime
from pathlib import Path
from lxml import html
import os, requests
import urllib
import pandas as pd
# set paths
dir_path = os.path.dirname(os.path.dirname(os.getcwd()))
data_path = os.path.join(dir_path, 'data')
raw_path = os.path.join(data_path, 'raw')

# set url
url = "https://m.rbi.org.in/scripts/WSSViewDetail.aspx?TYPE=Section&PARAM1=7"

# get data links from website
main_session = requests.Session()
response = main_session.get(url)
tree = html.fromstring(response.content)

xls_links = [a for a in tree.xpath('//table[@class="tablebg"]//td//a/@href') if a.endswith('XLSX')]
len_links = len(xls_links)
dates = [a for a in tree.xpath('//table[@class="tablebg"]//tr//th/text()')][:len_links]
main_session.close()

# Create a dictionary with dates as keys and links as values
date_link_dict = dict(zip(dates, xls_links))

# Download the files
for date, link in date_link_dict.items():
    session = requests.Session()
    print(f"Downloading {link}")
    resp = session.get(link)
    # Format the filename based on the date
    filename_date = date.replace(" ", "_")
    local_excel_path = os.path.join(raw_path, f"{filename_date}.xlsx")
    with open(local_excel_path, 'wb') as f:
        f.write(resp.content)
    print(f"Saved to {local_excel_path}")
    session.close()