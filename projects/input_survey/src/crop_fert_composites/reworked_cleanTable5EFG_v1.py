# %%
import pandas as pd
from pathlib import Path
import re
import io

# %%
PROJECT_DIR = Path(__file__).parent.parent
DATA_DIR = PROJECT_DIR / "2016"
DESTINATION_DIR = PROJECT_DIR / "interim"
BASE_COLUMNS = ["survey_year", "table_name", "state_name", "district_name"]
# %%
# Taking all 5 series tables in csv_files 
csv_filesE = list(DATA_DIR.rglob("**/TABLE5E*.csv"))
csv_filesF = list(DATA_DIR.rglob("**/TABLE5F*.csv"))
csv_filesG = list(DATA_DIR.rglob("**/TABLE5G*.csv"))

csv_files = csv_filesE + csv_filesF + csv_filesG

# %%

def extract_pages(p: Path) -> list[pd.DataFrame]:
    with open(p, 'r') as file:
        raw_text = file.read()
    text = re.sub(r'\n{3,}', '\n\n', raw_text)
    text_pages = list(
        filter(lambda x: x != "", map(str.strip, text.split("\n\n"))))
    assert len(text_pages) == 2, "Table 5EFG should have 2 pages"
    pages = [pd.read_csv(io.StringIO(p)) for p in text_pages]
    # We cannot assert the number of rows because for each district it varies.
    return pages[-1:]

# %%


def test_extract_pages():
    for p in csv_files:
        extract_pages(csv_files[0])


# %%
def name_base_columns(df: pd.DataFrame):
    return df
# %%


def join_pages(pages: list[pd.DataFrame]):

    num_pages = map(lambda p: p[p.columns.difference(BASE_COLUMNS)], pages)
    wide_df = pd.concat(num_pages, axis=1)
    # We cannot assert the number of rows because for each district it varies.
   
    return wide_df

# %%


def test_join_pages():
    for p in csv_files:
        join_pages(extract_pages(p))


# %%
column_name_mapping = {
    'col15':'SIZE GROUP(HA)', 
    'col16':'NO. OF HOLDINGS GROWING CROPS_TOTAL NO.', 
    'col17':'NO. OF HOLDINGS GROWING CROPS_NO. TREATED WITH MANURE', 
    'col18':'AREA UNDER_HYV',
    'col141':'AREA UNDER_HYB',
    'col19':'AREA UNDER_OTHERS',
    'col20':'AREA UNDER_TOTAL',
    'col21':'AREA TREATED WITH MANURE_HYV',
    'col151':'AREA TREATED WITH MANURE_HYB',
    'col22':'AREA TREATED WITH MANURE_OTHERS',
    'col23':'AREA TREATED MANURE_TOTAL',
    'col24':'TOTAL MANURE APPLIED_HYV',
    'col161':'TOTAL MANURE APPLIED_HYB',
    'col25':'TOTAL MANURE APPLIED_OTHERS',
    'col26':'TOTAL MANURE APPLIED_TOTAL' 
}

# %%


def main():

    for i in range(len(csv_files)):
        df = join_pages(map(name_base_columns, extract_pages(csv_files[i])))
        df = df.rename(columns=column_name_mapping)
        # TODO: handle Unnamed columns
        df = df.drop(columns= {'col14'},axis=1)
        dest = DESTINATION_DIR / "2016" / csv_files[i].relative_to(DATA_DIR).parent
        dest.mkdir(parents=True, exist_ok=True)
        df.to_csv(DESTINATION_DIR / "2016" / csv_files[i].relative_to(DATA_DIR), index=False)


if __name__ == "__main__":
    main()