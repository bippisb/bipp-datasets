import pandas as pd
from itertools import chain
from pathlib import Path


def extract_rows_with_data_points(df: pd.DataFrame):
    index = df.iloc[:, 2:].dropna(how='all').index
    return df.iloc[index].reset_index(drop=True)


def find_crores_row_idx(df: pd.DataFrame):
    """Returns the row index where 'in Crores' is mentioned in the head of the dataframe"""
    for idx, row in df.iterrows():
        if "crore" in str(row.values).lower():
            return idx


def find_major_head_cell(df: pd.DataFrame) -> tuple[int, str]:
    """Returns the location of the cell containing the 'Major Head' title"""
    def criterion(series): return "major head" in str(series).lower()
    row_mask = df.apply(criterion, axis=1)
    col_mask = df.apply(criterion)
    cell = df.loc[row_mask, col_mask]

    assert len(cell.columns) == 1, "There should be only one major head column"
    assert len(cell.index) == 1, "There should be only one major head row"

    return cell.index.min(), cell.columns[0]


def load_tax_receipts(fp: Path):
    raw = pd.read_excel(fp).dropna(axis=1, how="all").dropna(
        axis=0, how="all").reset_index(drop=True)
    in_crores_title_row_idx = find_crores_row_idx(raw)
    cln = raw[in_crores_title_row_idx + 1:].dropna(axis=1, how="all")
    assert len(cln.columns) == 8, "There should be 8 columns in the dataframe"
    return cln.reset_index(drop=True)


def extract_section_mapping(df: pd.DataFrame):
    def is_section_number(x): return isinstance(
        x, int) or (isinstance(x, str) and str.isnumeric(x))
    mask = df.iloc[:, 0].str.strip(".").apply(is_section_number)
    heads = df.loc[mask].iloc[:, :2]
    return dict(zip(heads.iloc[:, 0], heads.iloc[:, 1]))


def get_rows_with_serial_number(df: pd.DataFrame) -> pd.Index:
    return df.loc[df.iloc[:, 1].str.strip(".").str.contains(".", na=False, regex=False)].index


def get_rows_with_major_head_number(df: pd.DataFrame, major_head_col: str) -> pd.Index:
    return df.loc[2:, major_head_col].dropna().index


def filter_datapoints_with_serial_and_head_no(df: pd.DataFrame, major_head_col: str):
    mask = list(set(i for i in chain(
        get_rows_with_serial_number(df),
        get_rows_with_major_head_number(df, major_head_col=major_head_col)
    )))
    return df.iloc[mask]




def filter(df: pd.DataFrame):
    df = df.dropna(axis=0, how='all').dropna(axis=1, how='all')
    col_df = df.loc[:, df.map(
        lambda x: isinstance(x, int)).any()].iloc[1:4, -5:]
    col_df.replace(pd.NA, '', inplace=True)
    irow = pd.concat([col_df.iloc[0:3].astype(str).agg(' '.join), col_df.iloc[3:]]).dropna(
        axis=0, how='all').dropna(axis=1, how='all')
    heads = df.loc[df.iloc[:, 0].apply(lambda x: isinstance(x, int))]
    heads = heads.iloc[:, :2]
    heads.columns = ['key', 'head']
    heads = heads.reset_index(drop=True)
    heads_dict = dict(zip(heads['key'], heads['head']))
    sub_heads = df.loc[df.iloc[:, 1].str.strip(".").str.contains(
        ".", na=False, regex=False)].iloc[:, 1:]
    sub_heads.columns = ['heads', 'subhead'] + irow.iloc[:, 0].tolist()
    sub_heads['heads'] = sub_heads['heads'].astype(str).str.split('.').str[0]
    sub_heads['heads'] = pd.to_numeric(sub_heads['heads'], errors='coerce')
    sub_heads['heads'] = sub_heads['heads'].map(heads_dict)
    sub_heads = sub_heads.reset_index(drop=True)
    sub_heads.columns = sub_heads.columns.str.strip()

    return sub_heads


def split_variable_column(df: pd.DataFrame, column_name: str = "variable"):
    df['year'] = df[column_name].str.extract(r'(\d{4}-\d{4})')
    df['estimate_type'] = df[column_name].str.replace(
        r'\d{4}-\d{4}', '', regex=True).str.strip()
    return df.drop(columns=column_name)


def data_cleaning_pipeline(df: pd.DataFrame):
    return df.pipe(filter).drop(columns='Major Head')
