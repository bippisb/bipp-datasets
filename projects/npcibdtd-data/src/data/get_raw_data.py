import json
import urllib.request

import pandas as pd

# requesting data
with urllib.request.urlopen("https://www.npci.org.in/files/npci/TdBd.json") as url:
    result = json.loads(url.read().decode())


df = pd.DataFrame(result["data"])

# reorder columns

df_reorder = df[
    [
        "Year",
        "Month",
        "Product",
        "IssuerBankName",
        "TotalVolume",
        "ApprovedTransactionVolume",
        "BusinessDeclineTransactions",
        "TechnicalDeclineTransactions",
    ]
]

# converting to csv
df_reorder.to_csv(r"data\raw\npciBDTD.csv", index=False)
