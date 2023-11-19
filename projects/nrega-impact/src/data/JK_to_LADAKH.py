import pandas as pd

JK = pd.read_csv("./data/processed/JAMMU AND KASHMIR.csv")

# uppercasing the block names in state files
JK["block_name"] = JK["block_name"].str.upper()
JK["state"] = JK["state"].str.upper()
JK["district"] = JK["district"].str.upper()


# function to Extract and create Ladakh as a seperate state
def JK_to_Ladakh(data):
    data1 = data[data["district"].isin(["KARGIL", "LEH LADAKH"])]
    data1["state"] = "LADAKH"
    data2 = data[~data["district"].isin(["KARGIL", "LEH LADAKH"])]
    return data2, data1


new = []
new = JK_to_Ladakh(JK)

JK_new = new[0]
Ladakh = new[1]

# Exporting JK and Ladakh to processed as CSV file
JK_new.to_csv("./data/processed/JAMMU AND KASHMIR.csv", index=False)
Ladakh.to_csv("./data/processed/LADAKH.csv", index=False)
