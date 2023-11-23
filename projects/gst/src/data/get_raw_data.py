import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import os
import urllib3

reqUrl = "https://www.gst.gov.in/download/gststatistics"

headersList = {
 "Accept": "*/*",
 "User-Agent": "Thunder Client (https://www.thunderclient.com)" 
}

payload = ""

response = requests.request("GET", reqUrl, data=payload,  headers=headersList)

# Parse the HTML content of the page using BeautifulSoup
soup = BeautifulSoup(response.text, 'html.parser')

# Find all the <a> tags with a 'data-download' attribute
a_tags_with_data_download = soup.find_all('a', attrs={'data-download': True})

# Iterate through the <a> tags and download the linked files
for a_tag in a_tags_with_data_download:
    href = a_tag['href']

    # Check if the href is a valid web URL
    if not href.startswith('http://') and not href.startswith('https://'):
        # If it doesn't start with 'http://' or 'https://', add 'https://' to make it a valid URL
        href = urljoin(urllib3, href)

    file_name = os.path.basename(urlparse(href).path)  # Extract the file name from the URL

    # Download the file and save it to the 'downloaded_files' directory
    with open(os.path.join("../data/raw", file_name), 'wb') as file:
        file_content = requests.get(href).content
        file.write(file_content)