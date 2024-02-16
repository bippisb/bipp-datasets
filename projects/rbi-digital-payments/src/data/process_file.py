import polars as pl
import pandas as pd
import datetime
import numpy as np
from pathlib import Path

# Paths
data_dir = Path(__file__).resolve().parents[2]
raw_data = data_dir / "data" / "raw"
processed_data = data_dir / "data" / "processed"
raw_file = raw_data / "rbi_daily_digital_payments.xlsx"

# Functions
def str_count():
    """Generates an iterator that yields consecutive string representations of increasing integer values"""
    count = 0
    while True:
        yield str(count)
        count += 1

def old_extract_column_names(df: pl.DataFrame) -> list:
    """Extracts column names from a sheet in rbi_daily_digital_payments data release"""
    col_names = df.transpose(column_names=str_count()) \
        .with_columns([
            pl.col("3").forward_fill(),
            pl.col("4").forward_fill(limit=1)
        ]) \
        .select(
            pl.concat_str([
                pl.col('3'),
                pl.col('4').fill_null("")
            ], separator="_")
            .str.strip())["3"].drop_nulls()
    col_names = col_names \
        .str.to_lowercase() \
        .str.replace_all(r"[\s\-_]+", "_") \
        .str.replace_all(r"[\(\)]+", "") \
        .str.replace_all("/", "or") \
        .str.replace("prepaid_payment_instruments_ppis", "ppis") \
        .str.replace("_for_all_deals_matched_during_the_day", "").to_list()
    col_names = ["date"] + col_names
    return col_names

def new_extract_column_names(df: pl.DataFrame) -> list:
    """Extracts column names from a sheet in rbi_daily_digital_payments data release"""
    col_names = df.transpose(column_names=str_count()) \
        .with_columns([
            pl.col("3").forward_fill(),
            pl.col("4").forward_fill(limit=1)
        ]) \
        .select(
            pl.concat_str([
                pl.col('3'),
                pl.col('4').fill_null(""),
                pl.col("5")
            ], separator="_")
            .str.strip())["3"].drop_nulls()
    col_names = col_names \
        .str.to_lowercase() \
        .str.replace_all(r"[\s\-_]+", "_") \
        .str.replace_all(r"[\(\)]+", "") \
        .str.replace_all("/", "or") \
        .str.replace("prepaid_payment_instruments_ppis", "ppis") \
        .str.replace("_for_all_deals_matched_during_the_day", "").to_list()
    col_names = ["date"] + col_names
    return col_names

def strip_notes(df: pd.DataFrame):
    """Drop the rows that contains notes"""
    totals_rows = df.index[df.iloc[:, 0].str.lower().str.contains(
        "total", na=False, case=False)].tolist()
    notes_rows = df.index[df.iloc[:, 0].str.lower().str.contains(
        "note:", na=False, case=False)].tolist()
    strip_idx = totals_rows[0] if len(totals_rows) else notes_rows[0] - 1
    return df.iloc[:strip_idx].copy().reset_index(drop=True)

def new_process_data(df: pl.DataFrame) -> pl.DataFrame:
    """Process the raw data and returns a dataframe"""
    # df = pl.from_pandas(data)
    # prepare column names
    col_names = new_extract_column_names(df)
    print(col_names)
    df = df[6:]
    # drop columns where all values are null
    df = df[[s.name for s in df if not (s.null_count() == df.height)]]
    # assign column names
    df.columns = col_names
    df = df.to_pandas().pipe(strip_notes)
    # parse values
    df = df.replace('h', None)
    df = pl.from_pandas(df)
    df = df.with_columns(
        pl.col("*").exclude("date").str.strip().cast(pl.Float64),
        pl.col("date").str.strip().str.strptime(pl.Date, r"%B %d, %Y").dt.strftime("%d-%m-%Y")
    )
    return df

def old_process_data(df: pl.DataFrame) -> pl.DataFrame:
    """Process the raw data and returns a dataframe"""
    # df = pl.from_pandas(data)
    # prepare column names
    col_names = old_extract_column_names(df)
    print(col_names)
    df = df[5:]
    # drop columns where all values are null
    df = df[[s.name for s in df if not (s.null_count() == df.height)]]
    # assign column names
    df.columns = col_names
    df = df.to_pandas().pipe(strip_notes)
    # parse values
    df = df.replace('h', None)
    df = pl.from_pandas(df)
    df = df.with_columns(
        pl.col("*").exclude("date").str.strip().cast(pl.Float64),
        pl.col("date").str.strip().str.strptime(pl.Date, r"%B %d, %Y").dt.strftime("%d-%m-%Y")
    )
    return df

# Getting Sheet Names from Excel
sheets = pd.ExcelFile(str(raw_file)).sheet_names
# Read the Excel file content
dfs =[pl.read_excel(str(raw_file), sheet_name=sheet, ) for sheet in sheets]

# Process the data
dfs1 =[]
for i in range(len(sheets)):
    if i < 16:
        df = old_process_data(dfs[i])
        print(df.columns)
        dfs1.append(df)
    else:
        df = new_process_data(dfs[i])
        print(df.columns)
        dfs1.append(df)

# Concatenate the processed data
result_df = pl.concat(dfs1, how="diagonal")
# Convert to Pandas
df = result_df.to_pandas()
# Replace NaN values with None
df = df.replace({np.nan: None})
# todays date
date = datetime.date.today().strftime("%d-%m-%Y").replace("-", "_")
filename = f"rbi_daily_digital_payments_{date}.csv"
processed_file = processed_data / filename
# save as csv file
df.to_csv(str(processed_file), index=False)