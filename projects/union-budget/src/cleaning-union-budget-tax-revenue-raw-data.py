import pandas as pd
from pathlib import Path
import re
import numpy as np


DATA_DIR = Path(__file__).parents[1] / "data"
DEST_PATH = DATA_DIR / "interim" / "ub__last_4_tax_revenue_years.csv"

files = list((DATA_DIR / "raw" / "tax revenue").glob("*.xlsx"))

xls = [pd.read_excel(f).dropna(axis=1, how="all").dropna(axis=0, how="all") for f in files]

# Function to check if a value is a subheading of another
def is_subheading(value, next_value):
    return next_value.startswith(value) and next_value != value

# Function to clean and format raw Excel data
def format_raw_xls_data(xls):
    for df in xls:
        # Remove columns with all NaN values
        cols_to_remove = df.columns[1:3][(df.iloc[:, 1:3].isna().all(axis=0))]
        df.drop(columns=cols_to_remove, inplace=True)

        cols_to_remove = df.columns[1:2][(df.iloc[1:10, 1:2].isna().all(axis=0))]
        df.drop(columns=cols_to_remove, inplace=True)

        # Reset index and remove rows where the next row is a subheading
        df.reset_index(drop=True, inplace=True)
        index_to_remove = []

        for i in range(len(df) - 1):
            current_value = df.iloc[i, 1]  
            next_value = df.iloc[i + 1, 1]

            if pd.notna(current_value) and pd.notna(next_value):
                if is_subheading(current_value, next_value):
                    index_to_remove.append(i)

        df.drop(index_to_remove, inplace=True)
        df.reset_index(drop=True, inplace=True)

# Function to extract subheadings from a DataFrame
def extract_subheads(df):
    subheads = df.loc[df.iloc[:, 1].str.strip(".").str.contains(".", na=False, regex=False)].iloc[:, :]
    return subheads.iloc[:, 1:]

# Function to extract header information from a DataFrame
def extract_header(df):
    merged_df = pd.DataFrame(columns=df.columns)
    df.reset_index(drop=True, inplace=True)

    major_head_indices = df[df.iloc[:, 3] == "Major Head"].index  
    first_group = df.iloc[:major_head_indices[0] + 1]

    merged_row = first_group.apply(lambda x: ' '.join(x.dropna().astype(str)), axis=0)
    merged_row = merged_row.to_frame().T
    merged_df = pd.concat([merged_df, merged_row], ignore_index=True)

    secgroup = df.iloc[major_head_indices[0] + 1:, :]
    df = pd.concat([merged_row, secgroup], ignore_index=True)
    
    # Cleaning: Replace '\n', 'crores', 'Crores', 'In', '(', ')' in the first row
    df.iloc[0] = df.iloc[0].str.replace(r'\n', ' ').str.replace(r'crores', ' ', regex=True).str.replace(r'Crores', ' ', regex=True).str.replace(r'In', ' ', regex=True).str.replace(r'\(', ' ', regex=True).str.replace(r'\)', ' ', regex=True)
    
    return df.iloc[0]

# Function to extract total head information from a DataFrame
def extract_total_head(df):
    sdf = df[df.iloc[:, 0].notna()]
    sdf = sdf[sdf.iloc[:, -1].notna()]
    sdf = sdf[sdf.iloc[:, -2].notna()]
    sdf = sdf[sdf.iloc[:, -3].notna()]
    sdf = sdf[sdf.iloc[:, 3].isna()]
    # Rename columns and clean: Replace 'Total-', 'Net-' in the first column
    sdf.iloc[:, 1] = sdf.iloc[:, 0]
    sdf.iloc[:, 0] = sdf.iloc[:, 0].str.replace('Total-', '').str.replace('Net-', '')
    sdf = sdf.drop(sdf.columns[3], axis=1)
    return sdf

# Function to extract heads from a DataFrame
def extract_heads(df):
    numeric_values = pd.to_numeric(df.iloc[:, 0], errors='coerce')
    result_df = df[~numeric_values.isna()]
    return result_df

# Function to map subheadings to heads in a DataFrame
def mapping_subtohead(df):
    heads = extract_heads(df)
    heads = heads.iloc[:, :2]
    heads.columns = ['key', 'head']

    heads['key'] = heads['key'].astype(str).str.split('.').str[0]
    heads['key'] = pd.to_numeric(heads['key'], errors='coerce')
    heads_dict = dict(zip(heads['key'], heads['head']))
    
    subheads = extract_subheads(df)
    subheads.columns = ['Head', 'subhead'] + list(extract_header(df).iloc[3:])
    subheads['Head'] = subheads['Head'].astype(str).str.split('.').str[0]
    subheads[subheads.columns[0]] = pd.to_numeric(subheads[subheads.columns[0]], errors='coerce')
    
    total_head = extract_total_head(df)
    total_head.columns = subheads.columns
    subheads['Head'] = subheads['Head'].map(heads_dict)
    result_df = pd.concat([subheads, total_head])
    return result_df
    # Sort DataFrame by 'Head' column
    # sorted_df = result_df.sort_values(by=['Head'])
    
    # return sorted_df

# Function to split variable column and clean data in a DataFrame
def split_variable_column(df: pd.DataFrame, column_name: str = "variable"):
    df['year'] = df[column_name].str.extract(r'(\d{4}-\d{4})')
    
    # Extract 'estimate_type' and clean
    df['estimate_type'] = df[column_name].str.replace(r'\d{4}-\d{4}', '', regex=True).str.strip()
    df['estimate_type'] = df['estimate_type'].str.extract(r'`?(.*)').squeeze().str.strip()
    
    return df.drop(columns=column_name)

# Function for the entire data cleaning pipeline
def data_cleaning_pipeline(df: pd.DataFrame):
    return df.pipe(mapping_subtohead).drop(columns='Major Head')

# Main function
def main():
    # Read and format raw Excel data
    files = list((DATA_DIR / "raw" / "tax revenue").glob("*.xlsx"))
    xls = [pd.read_excel(f).dropna(axis=1, how="all").dropna(axis=0, how="all") for f in files]

    # Perform data cleaning
    format_raw_xls_data(xls)
    
    # Map subheadings to heads, melt DataFrame, and split variable column
    cln_dfs = map(lambda df: df.pipe(data_cleaning_pipeline).melt(id_vars=['Head', 'subhead']).pipe(split_variable_column), xls)

    # Concatenate cleaned DataFrames
    df = pd.concat(cln_dfs)
    
    # Select columns and convert 'value' column to numeric
    col_order = ["year", "estimate_type", "Head", "subhead", "value"]
    fnl_df = df[col_order]
    fnl_df['value'] = pd.to_numeric(fnl_df['value'], errors='coerce')

    # Drop rows where 'value' column is NaN
    fnl_df = fnl_df.dropna(subset=['value'])

    # Save the final DataFrame to CSV
    fnl_df.to_csv(DEST_PATH, index=False)

if __name__ == "__main__":
    main()