# %%
import pandas as pd
from pathlib import Path
import re
import io

# %%
PROJECT_DIR = Path(__file__).parent.parent
DATA_DIR = PROJECT_DIR / "2016"
DESTINATION_DIR = PROJECT_DIR / "interim"
BASE_COLUMNS = ["survey_year", "state_name", "district_name"]
# %%
csv_files = list(DATA_DIR.rglob("**/TABLE2A*.csv"))

# %%


def extract_pages(p: Path) -> list[pd.DataFrame]:
    with open(p, 'r') as file:
        raw_text = file.read()
    text = re.sub(r'\n{3,}', '\n\n', raw_text)
    text_pages = list(
        filter(lambda x: x != "", map(str.strip, text.split("\n\n"))))
    assert len(text_pages) == 1, "Table 2A should have 1 page"
    pages = [pd.read_csv(io.StringIO(p)) for p in text_pages]
    # for p in pages:
    #     assert len(p) == 6, "Each page should have 6 rows"
    # REMOVED BECAUSE PUNJAB HAS INCOMPLETE DATA TABLE2A 
    return pages

# %%


def test_extract_pages():
    for p in csv_files:
        extract_pages(csv_files[0])


# %%
def name_base_columns(df: pd.DataFrame):
    textbox_cols = df.columns[df.columns.str.contains("Textbox")]
    assert len(textbox_cols) == 3, "Table 2A should have 3 textboxes"

    df = df.rename(columns=dict(zip(df.columns[:3], BASE_COLUMNS)))
    return df
# %%


def join_pages(pages: list[pd.DataFrame]):

    num_pages = map(lambda p: p[p.columns.difference(BASE_COLUMNS)], pages)
    wide_df = pd.concat(num_pages, axis=1)
    # assert wide_df.shape[0] == 6, "Concatenated dataframe should also have 6 rows, just like the individual pages."
    # REMOVED BECAUSE PUNJAB HAS INCOMPLETE DATA TABLE2A 
    
    return wide_df

# %%


def test_join_pages():
    for p in csv_files:
        join_pages(extract_pages(p))


# %%
column_name_mapping = {
        'col2':'SIZE GROUP(HA)',
        'col3':'IrriArea_CroppedOnce',
        'col4':'IrriArea_CroppedTwice_OneCropIrri',
        'col5':'IrriArea_CroppedTwice_TwoCropIrri',
        'col6':'IrriArea_CroppedMore_OneCropIrri',
        'col7':'IrriArea_CroppedMore_TwoCropIrri',
        'col8':'IrriArea_CroppedMore_MoreCropIrri',
        'col9':'Net Irrigated Area',
        'col10':'Gross Irrigated Area'
}
# %%


def main():
    for i in range(len(csv_files)):
        print(csv_files[i])
        df = join_pages(map(name_base_columns, extract_pages(csv_files[i])))
        df = df.rename(columns=column_name_mapping)
        # TODO: handle Unnamed columns
        df = df.drop(columns= {'col1'},axis=1)
        dest = DESTINATION_DIR / "2016" / csv_files[i].relative_to(DATA_DIR).parent
        dest.mkdir(parents=True, exist_ok=True)
        df.to_csv(DESTINATION_DIR / "2016" / csv_files[i].relative_to(DATA_DIR), index=False)


if __name__ == "__main__":
    main()

