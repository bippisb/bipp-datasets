from pathlib import Path
import pandas as pd

# Specify the path to your folder containing CSV files
RAW_DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "raw" / "daily_retail_prices"
OUT_FILE = Path(__file__).resolve().parents[1] / "data" / "interim" / "consolidated_India_level_daily_retail_prices.csv"

# Initialize an empty DataFrame to store the final result
result_df = pd.DataFrame()

# Loop through each CSV file in the folder
for file in RAW_DATA_DIR.glob("*.csv"):
    # Extract date from the file name
    date_str = file.stem.split("_")[2]
    date = pd.to_datetime(date_str, format='%d-%m-%Y')
    date = date.strftime('%d-%m-%Y')
    
    # Read the CSV file into a DataFrame
    df = pd.read_csv(file)
    
    # Exclude the state-wise data
    df = df.iloc[:4]

    # Transpose the DataFrame
    df = df.transpose()

    # Reset index to get the commodity as a column
    df.reset_index(inplace=True)

    # Use the first row as column names
    df.columns = df.iloc[0]

    # Skip the first row after transposing
    df = df.iloc[1:]

    # Rename the column with 'States/UTs' to 'Commodity'S
    df = df.rename(columns={'States/UTs': 'Commodity'})
    
    # Add a 'Date' column with the extracted date
    df.insert(0, 'Date', date)
    
    # Append the reshaped DataFrame to the result DataFrame
    result_df = pd.concat([result_df, df], ignore_index=True)

# Save the final result to a new CSV file or perform further analysis as needed
result_df.to_csv(OUT_FILE, index=False)
