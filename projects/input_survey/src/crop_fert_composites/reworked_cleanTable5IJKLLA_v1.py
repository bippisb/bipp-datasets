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
csv_filesI = list(DATA_DIR.rglob("**/TABLE5I*.csv"))
csv_filesJ = list(DATA_DIR.rglob("**/TABLE5J*.csv"))
csv_filesK = list(DATA_DIR.rglob("**/TABLE5K*.csv"))
csv_filesL = list(DATA_DIR.rglob("**/TABLE5L*.csv"))
csv_filesLA = list(DATA_DIR.rglob("**/TABLE5LA*.csv"))

csv_files = csv_filesI + csv_filesJ + csv_filesK + csv_filesL +csv_filesLA


# %%

def extract_pages(p: Path) -> list[pd.DataFrame]:
    with open(p, 'r') as file:
        raw_text = file.read()
    text = re.sub(r'\n{3,}', '\n\n', raw_text)
    text_pages = list(
        filter(lambda x: x != "", map(str.strip, text.split("\n\n"))))
    assert len(text_pages) == 2, "Table 5IJKLLA should have 2 pages"
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
    'col12':'SIZE GROUP(HA)', 
    'col13':'NO. OF HOLDINGS GROWING CROPS_TOTAL NO.', 
    'col14':'NO. OF HOLDINGS GROWING CROPS_NO. TREATED WITH MANURE', 
    'col15':'AREA UNDER_HYV',
    'col141':'AREA UNDER_HYB',
    'col16':'AREA UNDER_OTHERS',
    'col17':'AREA UNDER_TOTAL',
    'col18':'AREA TREATED WITH MANURE_HYV',
    'col151':'AREA TREATED WITH MANURE_HYB',
    'col19':'AREA TREATED WITH MANURE_OTHERS',
    'col20':'AREA TREATED MANURE_TOTAL'
}

# %%


def main():

    for i in range(len(csv_files)):
        df = join_pages(map(name_base_columns, extract_pages(csv_files[i])))
        df = df.rename(columns=column_name_mapping)
        # TODO: handle Unnamed columns
        df = df.drop(columns= {'col11'},axis=1)
        dest = DESTINATION_DIR / "2016" / csv_files[i].relative_to(DATA_DIR).parent
        dest.mkdir(parents=True, exist_ok=True)
        df.to_csv(DESTINATION_DIR / "2016" / csv_files[i].relative_to(DATA_DIR), index=False)

if __name__ == "__main__":
    main()