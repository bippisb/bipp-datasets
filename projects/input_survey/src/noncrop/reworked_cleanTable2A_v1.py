# %%
import pandas as pd
from pathlib import Path
import re
import io

# %%
# Define project directories
PROJECT_DIR = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_DIR / "data"/"raw"
DESTINATION_DIR = PROJECT_DIR / "data"/"interim"
BASE_COLUMNS = ["survey_year", "state_name", "district_name"]

# %%
# Create a list of CSV files from TABLE2A
csv_files = list(DATA_DIR.rglob("**/TABLE2A*.csv"))

# %%
# Define a function to extract pages from a CSV file
def extract_pages(p: Path) -> list[pd.DataFrame]:
    with open(p, 'r') as file:
        raw_text = file.read()
    # Preprocess text by removing excessive newlines
    text = re.sub(r'\n{3,}', '\n\n', raw_text)
    # Split the text into pages based on double newline
    text_pages = list(filter(lambda x: x != "", map(str.strip, text.split("\n\n"))))
    # Currently, no assertion on the number of rows in each page to handle incomplete data in Punjab
    pages = [pd.read_csv(io.StringIO(p)) for p in text_pages]
    return pages

# %%
# Define a function to test the extraction of pages
def test_extract_pages():
    for p in csv_files:
        extract_pages(csv_files[0])

# %%
# Define a function to name base columns
def name_base_columns(df: pd.DataFrame):
    # Identify columns containing 'Textbox'
    textbox_cols = df.columns[df.columns.str.contains("Textbox")]
    # Assert that Table 2A should have 3 textboxes
    assert len(textbox_cols) == 3, "Table 2A should have 3 textboxes"
    # Rename the columns with base columns
    df = df.rename(columns=dict(zip(df.columns[:3], BASE_COLUMNS)))
    return df

# %%
# Define a function to join pages into a wide DataFrame
def join_pages(pages: list[pd.DataFrame]):
    # Extract columns excluding base columns
    num_pages = map(lambda p: p[p.columns.difference(BASE_COLUMNS)], pages)
    # Concatenate pages horizontally
    wide_df = pd.concat(num_pages, axis=1)
    return wide_df

# %%
# Define a function to test the joining of pages
def test_join_pages():
    for p in csv_files:
        join_pages(extract_pages(p))

# %%
# Mapping of column names after renaming
column_name_mapping = {
    'col2': 'SIZE GROUP(HA)',
    'col3': 'IrriArea_CroppedOnce',
    'col4': 'IrriArea_CroppedTwice_OneCropIrri',
    'col5': 'IrriArea_CroppedTwice_TwoCropIrri',
    'col6': 'IrriArea_CroppedMore_OneCropIrri',
    'col7': 'IrriArea_CroppedMore_TwoCropIrri',
    'col8': 'IrriArea_CroppedMore_MoreCropIrri',
    'col9': 'Net Irrigated Area',
    'col10': 'Gross Irrigated Area'
}
# %%
def main():
    for i in range(len(csv_files)):
        df = join_pages(map(name_base_columns, extract_pages(csv_files[i])))
        df = df.rename(columns=column_name_mapping)
        df = df.drop(columns={'col1'}, axis=1)
        dest = DESTINATION_DIR / "2016" / csv_files[i].relative_to(DATA_DIR).parent
        dest.mkdir(parents=True, exist_ok=True)
        df.to_csv(DESTINATION_DIR / "2016" / csv_files[i].relative_to(DATA_DIR), index=False)

# %%
if __name__ == "__main__":
    main()
