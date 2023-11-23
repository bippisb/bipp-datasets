import requests
from bs4 import BeautifulSoup
import pandas as pd

reqUrl = "https://www.npci.org.in/what-we-do/upi/product-statistics"

headersList = {
    "Accept": "*/*",
    "User-Agent": "Thunder Client (https://www.thunderclient.com)"
}

payload = ""

response = requests.request("GET", reqUrl, data=payload, headers=headersList)

# Check if the response status code is 200 (OK)
if response.status_code == 200:
    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find the table(s) in the HTML content
    tables = soup.find_all('table')
    
    # Check if tables were found
    if tables:
        # Convert the first table to a DataFrame
        df = pd.read_html(str(tables[0]))[0]

        # Add date column to the DataFrame
        df['Date'] = pd.to_datetime('01' + '-' + df['Month'], format='%d-%b-%y', errors='coerce')
        df['Date'] = df['Date'].dt.strftime('%d-%m-%Y')

        df = df[['Date', 'No. of Banks live on UPI', 'Volume (in Mn)', 'Value (in Cr.)']]
        df.columns = ['date', 'num_of_banks_live_on_upi', 'volume', 'value']
        
        # Save the DataFrame to a CSV file
        df.to_csv('../data/raw/npci_product_stats.csv', index=False)

        # Now, you have a DataFrame 'df' representing the table
        print(df)
    else:
        print("No tables found in the HTML content.")
else:
    print(f"Failed to fetch data. Status code: {response.status_code}")
