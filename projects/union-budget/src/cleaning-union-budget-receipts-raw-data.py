import pandas as pd
import re
from pathlib import Path
import csv

DATA_DIR = Path(__file__).parents[1] / "data"
DEST_PATH = DATA_DIR / "interim" / "ub_reciepts_last_4_years.csv"

files = list((DATA_DIR / "raw" / "union budget receipts raw data 2018-2022").glob("*.xlsx"))
xls = [pd.read_excel(f).dropna(axis=1, how="all").dropna(axis=0, how="all") for f in files]
def extract_rows_with_data_points(df: pd.DataFrame):
    index = df.iloc[:,2:].dropna(how='all').index
    return df.iloc[index].reset_index(drop=True)


def assign_column_names(df: pd.DataFrame):
    df.columns = ["hindi_head", "english_head"] + df.iloc[0, 2:].to_list()
    return df.iloc[1:].reset_index(drop=True)


def remove_utf8(text):
    if isinstance(text, str):
        # Use regular expression to remove non-UTF-8 characters
        return ''.join(char for char in text if char.isascii())
    else:
        return text  # Return non-string values as is


def remove_newlines(text):
    if isinstance(text, str):
        # Replace "\n" with an empty string
        return text.replace("\n", "")
    else:
        return text  # Return non-string values as is


def remove_characters_from_columns(df):
    for column in df.columns[1:]:
        df[column] = df[column].replace('[=\\-]', '', regex=True)
        df[column] = df[column].replace('.*\\.', '', regex=True)
    return df

def remove_brackets(text):
    if isinstance(text, str):
        # Use regular expression to remove content within both parentheses and square brackets
        return re.sub(r'\([^)]*\)|\[[^\]]*\]|=', '', text)

    else:
        return text  # Return non-string values as is


def data_cleaning_pipeline(df: pd.DataFrame):
    return df.pipe(extract_rows_with_data_points).map(remove_newlines).map(remove_brackets).map(remove_utf8).pipe(assign_column_names).drop(columns="hindi_head")


def split_variable_column(df: pd.DataFrame, column_name: str = "variable"):
    df['year'] = df[column_name].str.extract(r'(\d{4}-\d{4})')
    df['estimate_type'] = df[column_name].str.replace(r'\d{4}-\d{4}', '', regex=True).str.strip()
    return df.drop(columns=column_name)


cln_dfs = map(lambda df: df.pipe(data_cleaning_pipeline).melt(id_vars="english_head").pipe(split_variable_column), xls)


df = pd.concat(cln_dfs)
col_order = ["year", "estimate_type", "english_head", "value"]
df["english_head"] = df["english_head"].str.strip()
fnl_df = df[col_order]

# fnl_df = remove_characters_from_columns(fnl_df)
fnl_df.to_csv(DEST_PATH, index=False, quoting= csv.QUOTE_NONNUMERIC)

