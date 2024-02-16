#%%
import pandas as pd
import numpy as np
import tabula
import re
import os

# Define the path to the raw folder containing PDF files
base_dir = os.path.dirname(os.path.dirname(os.getcwd()))
raw_folder = os.path.join(base_dir, "data", "raw")
interim_folder = os.path.join(base_dir, "data", "interim")
#%%
# List all PDF files in the raw folder
pdf_files = [file for file in os.listdir(raw_folder) if file.endswith(".pdf")]

# Iterate through each PDF file
for filename in pdf_files:
    # Read the PDF
    pdf = tabula.read_pdf(os.path.join(raw_folder, filename), pages=1, lattice=True)
    df = pdf[0]

    # Clean the data by selecting columns with names containing "Airport" and "Freight"
    df = df[df.columns[df.columns.str.contains('Airport|Freight', case=False)]]
    df.columns = ['airport', 'freight']

    # Drop NaN values from Airport
    df = df.dropna(subset=['airport'])

    # Remove rows with numeric values in the 'Airport' column
    df = df[~df['airport'].str.match(r'^[0-9,]+$')]

    # Remove Hindi characters from the 'Airport' column
    df['airport'] = df['airport'].str.replace(r'[^\x00-\x7F]+', '', regex=True)

    # Extract date from the filename
    date_match = re.search(r'([A-Za-z]+)(\d+k\d{2,4})', filename)
    if date_match:
        month = date_match.group(1)
        year = date_match.group(2).replace('k', '0')
        # Adding a day to create a valid date
        formatted_date = f'01-{month}-{year}'
    else:
        formatted_date = ''

    # Add the extracted date as a new column
    df['date'] = pd.to_datetime(formatted_date, format='mixed')
    # Remove parentheses and their contents from 'Airport' column while trimming spaces
    df['airport'] = df['airport'].str.replace(r'\s*\([^)]*\)', '', regex=True).str.strip().str.lower()

    # Reset the index
    df.reset_index(drop=True, inplace=True)

    # Save the cleaned data to a CSV file
    csv_filename = os.path.splitext(filename)[0] + ".csv"
    df.to_csv(os.path.join(interim_folder, csv_filename), index=False)

    print(f"Processed {filename} and saved as {csv_filename}")

# %%
