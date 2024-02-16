import requests
from urllib.request import urlretrieve
from bs4 import BeautifulSoup as Soup
from pathlib import Path

data_dir = Path(__file__).resolve().parents[2]
raw_data = data_dir / "data" / "raw"

url = "https://rbi.org.in/Scripts/Statistics.aspx"

response = requests.get(url)
response.raise_for_status()
response = Soup(response.content)

file_link = response.find("a", string="Settlement Data of Payment Systems").attrs["href"]
print(f"File link: {file_link}")

filename = "rbi_daily_digital_payments.xlsx"
filepath = str(raw_data / filename)
# Download and save the file
urlretrieve(file_link, filepath)

print(f"Downloaded {filename}")