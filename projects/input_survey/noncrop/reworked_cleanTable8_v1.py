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
csv_files = list(DATA_DIR.rglob("**/TABLE8-*.csv"))

# %%


def extract_pages(p: Path) -> list[pd.DataFrame]:
    with open(p, 'r') as file:
        raw_text = file.read()
    text = re.sub(r'\n{3,}', '\n\n', raw_text)
    text_pages = list(
        filter(lambda x: x != "", map(str.strip, text.split("\n\n"))))
    assert len(text_pages) == 3, "Table 8 should have 3 pages"
    pages = [pd.read_csv(io.StringIO(p)) for p in text_pages]
    # for page in pages:
    #     # Punjab state data is not complete
    #     # assert len(p) == 6, "Each page should have 6 rows"
    #     if len(page) !=6:
    #         print("NOO",p)
    
    # The correct data required is present in the pages 1 & 3. 
    return [pages[0],pages[2]]

# %%


def test_extract_pages():
    for p in csv_files:
        extract_pages(csv_files[0])


# %%
def name_base_columns(df: pd.DataFrame):
    textbox_cols = df.columns[df.columns.str.contains("Textbox")]
    assert len(textbox_cols) == 4, "Table 4 should have 4 textboxes"

    df = df.rename(columns=dict(zip(df.columns[:4], BASE_COLUMNS)))
    return df
# %%


def join_pages(pages: list[pd.DataFrame]):

    num_pages = map(lambda p: p[p.columns.difference(BASE_COLUMNS)], pages)
    wide_df = pd.concat(num_pages, axis=1)
    # Punjab data is incomplete
    # assert wide_df.shape[0] == 6, "Concatenated dataframe should also have 6 rows, just like the individual pages."

    return wide_df

# %%


def test_join_pages():
    for p in csv_files:
        join_pages(extract_pages(p))


# %%
column_name_mapping = {
        'Col2':'SIZE GROUP(HA)', 
        'Col3':'TotalNumOfOperationalHolding', 
        'Col4':'EstNumOperationalHoldings_TookInstitutionalCredit', 
        'Col5':'TookInstiCredit_FromPACS',
        'Col6':'TookInstiCredit_FromPLDB/SLDB',
        'Col7':'TookInstiCredit_FromCBB',
        'Col8':'TookInstiCredit_FromRRBB',
        'Col9':'AmountCreditTaken(RS)_SL',
        'Col10':'AmountCreditTaken(RS)_ML',
        'Col11':'AmountCreditTaken(RS)_LL',
        'Col25':'TOTAL_SL', 
        'Col26':'TOTAL_ML', 
        'Col27':'TOTAL_LL',
        'Col28':'ShortTermLoan_ValueQuantityFert(Rs)',
        'Col29':'ShortTermLoan_ValueQuantityOthers',
        'Col30':'ShortTermLoan_GivenCash',
        'Col31':'ShortTermLoan_Total'
}
# %%


def main():
    for i in range(len(csv_files)):
        df = join_pages(map(name_base_columns, extract_pages(csv_files[i])))
        df = df.rename(columns=column_name_mapping)
        # TODO: handle Unnamed columns
        df = df.drop(columns= {'Col23','Col24','Col1'},axis=1)
        dest = DESTINATION_DIR / "2016" / csv_files[i].relative_to(DATA_DIR).parent
        dest.mkdir(parents=True, exist_ok=True)
        df.to_csv(DESTINATION_DIR / "2016" / csv_files[i].relative_to(DATA_DIR), index=False)


if __name__ == "__main__":
    main()

