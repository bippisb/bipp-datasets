import requests
import pandas as pd

reqUrl = "https://www.npci.org.in/files/npci/TdBd.json"

headersList = {
 "Accept": "*/*",
 "User-Agent": "Thunder Client (https://www.thunderclient.com)" 
}

payload = ""

response = requests.request("GET", reqUrl, data=payload,  headers=headersList)

# Check if the response status code is 200 (OK)
if response.status_code == 200:
    # Parse the JSON response
    data = response.json()['data']
    
    # Create a DataFrame from the JSON data
    df = pd.DataFrame(data)
    
    # Select the desired columns
    selected_columns = ['Product', 'IssuerBankName', 'TotalVolume', 'ApprovedTransactionVolume', 'BusinessDeclineTransactions', 'TechnicalDeclineTransactions', 'Month', 'Year']
    df = df[selected_columns]

    # Add date column to the DataFrame
    df['Date'] = pd.to_datetime('01' + '-' + df['Month'] + '-' + df['Year'], format='%d-%B-%Y')
    df['Date'] = df['Date'].dt.strftime('%d-%m-%Y')

    df = df[['Date', 'Product', 'IssuerBankName', 'TotalVolume', 'ApprovedTransactionVolume', 'BusinessDeclineTransactions', 'TechnicalDeclineTransactions']]
    df.columns = ['date', 'product', 'issuer_bank', 'total_volume', 'approved_transaction_volume', 'business_decline_transactions', 'technical_decline_transactions']

    # Save the DataFrame to a CSV file
    df.to_csv('../data/raw/npci_td_bd_data.csv', index=False)
        
    # Now, you have a DataFrame 'df' with the specified columns
    print(df)
else:
    print(f"Failed to fetch data. Status code: {response.status_code}")
