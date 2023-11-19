# %%
import camelot
from pathlib import Path
import pandas as pd
import numpy as np

# %%
payroll_report_pdf = Path.cwd().parent / "data" / "raw" / "payroll_reports" / "december_2022.pdf"

# %%
raw_tables = camelot.read_pdf(str(payroll_report_pdf), pages="all", flavor="lattice")

# %%
tables = list(map(lambda t: t.df.copy(), raw_tables))

# %%
def basic_cleaning(df: pd.DataFrame):
    df = df.dropna(how="all", axis=1)
    df = df.replace(r"[\r\n\s]+", " ", regex=True) \
        .replace('-', '', regex=True) \
        .replace("", np.nan)
    print("Load Table:", df.shape, "->", df.shape)
    return df

# %%
cln_tables = list(map(basic_cleaning, tables))

# %%
def is_monthly(df: pd.DataFrame):
    return df.iloc[0, :].str.contains("\w{3,9}\s\d{4}", regex=True).any()

# %%
def is_epf(df: pd.DataFrame):
    return df.iloc[:, 1].str.lower().str.contains('new EPF subscriber', na=False, case=False).any()

# %%
monthly_epf_tables = list(filter(lambda t: is_monthly(t) and is_epf(t), cln_tables))

# %%
def parse_date_head(df: pd.DataFrame) -> pd.Series:
    return pd.to_datetime(
        df.iloc[:, 0],
        format="%B %Y",
        errors="coerce"
    ).dropna()

# %%
def strip_month_headline(df: pd.DataFrame):
    date_df = parse_date_head(df)
    date_row_idx = date_df.index[0]
    return df.iloc[date_row_idx + 1:].reset_index(drop=True)


# %%
def exclude_totals_row(df: pd.DataFrame):
    return df[~df.iloc[:, 0].str.contains("total", na=False, case=False)]

# %%
def epf_correct_camelot(df: pd.DataFrame):
    # TODO: find a method to resolve this issue with camelot itself.
    df.iloc[7, [0,1]] = df.iloc[:, 0].str.extract(r'(.*) (\d[\d,.]*)$').iloc[7]
    df.iloc[2, [0,1]] = df.iloc[:, 1].str.extract(r'(.*) (\d[\d,.]*)$').iloc[2]
    return df

# %%
def prep_row_labels(df: pd.DataFrame):
    headings = df.iloc[:, 0].str.replace(r"(\d{2})(\d{2})", r"\1-\2", regex=True)
    headings[0:2] = ["head", "gender"]
    headings = headings.str.lower()
    return df.rename(index=headings).drop(0, axis=1)

# %%
def reshape_epf(df: pd.DataFrame):
    df.iloc[0].ffill(inplace=True)
    df = df.T.melt(id_vars=["head", "gender"], var_name="age")
    categorical_columns = ["head", "gender", "age"]
    df[categorical_columns] = df[categorical_columns].astype("category")
    # TODO: convert values to integer values
    # df.value = pd.to_numeric(df.value.str.replace(",", "").str.strip(), errors="coerce", downcast="unsigned")
    return df

# %%
df = monthly_epf_tables[0].pipe(strip_month_headline).pipe(exclude_totals_row).pipe(epf_correct_camelot).pipe(prep_row_labels).pipe(reshape_epf)
df

# %%
df.info()

# %%
parse_date_head(monthly_epf_tables[0]).iloc[0]

# %%
def epf_data_pipeline(df: pd.DataFrame):
    month = parse_date_head(df).iloc[0]
    df = df.pipe(strip_month_headline) \
        .pipe(exclude_totals_row) \
        .pipe(epf_correct_camelot) \
        .pipe(prep_row_labels) \
        .pipe(reshape_epf)
    df["year"] = month.year
    df["month"] = month.month
    return df

# %%
pd.concat(map(epf_data_pipeline, monthly_epf_tables))

# %%
