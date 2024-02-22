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
BASE_COLUMNS = ["survey_year", "table_name", "state_name", "district_name"]

# %%
# Get a list of CSV files for Table 9A
csv_files = list(DATA_DIR.rglob("**/TABLE9A-*.csv"))

# %%
# Function to extract pages from a CSV file
def extract_pages(p: Path) -> list[pd.DataFrame]:
    with open(p, 'r') as file:
        raw_text = file.read()
    # Normalize line breaks to handle variations
    text = re.sub(r'\n{3,}', '\n\n', raw_text)
    # Split the text into pages using double line breaks
    text_pages = list(filter(lambda x: x != "", map(str.strip, text.split("\n\n"))))
    # Ensure that there are exactly 4 pages (Punjab data is incomplete)
    assert len(text_pages) == 4, "Table 9A should have 4 pages"
    # Read the last two pages into DataFrames
    pages = [pd.read_csv(io.StringIO(p)) for p in text_pages[-2:]]
    return pages

# %%
# Function to test extracting pages
def test_extract_pages():
    for p in csv_files:
        extract_pages(csv_files[0])

# %%
# Function to name base columns
def name_base_columns(df: pd.DataFrame):
    # Identify and rename the first 4 columns with base columns
    textbox_cols = df.columns[df.columns.str.contains("Textbox")]
    assert len(textbox_cols) == 4, "Table 4 should have 4 textboxes"
    df = df.rename(columns=dict(zip(df.columns[:4], BASE_COLUMNS)))
    return df

# %%
# Function to join pages
def join_pages(pages: list[pd.DataFrame]):
    # Merge the DataFrames based on base columns (survey_year, table_name, state_name, district_name)
    num_pages = map(lambda p: p[p.columns.difference(BASE_COLUMNS)], pages)
    wide_df = pd.concat(num_pages, axis=1)
    return wide_df

# %%
# Function to test joining pages
def test_join_pages():
    for p in csv_files:
        join_pages(extract_pages(p))

# %%
# Column name mapping for the final dataframe
column_name_mapping = {
    'Col16': 'SIZE GROUP(HA)', 
    'Col28': 'TotalNumOfOperationalHolding', 
    'Col29': 'NumOfHoldUsingCertSeeds', 
    'Col63': 'NumOfHoldUsingHybridSeeds'
}

# %%
def main():
    for i in range(len(csv_files)):
        df = join_pages(map(name_base_columns, extract_pages(csv_files[i])))
        df = df.rename(columns=column_name_mapping)
        # TODO: handle Unnamed columns
        df = df.drop(columns={'Col15', 'Col39', 'Col40'}, axis=1)
        dest = DESTINATION_DIR / "2016" / csv_files[i].relative_to(DATA_DIR).parent
        dest.mkdir(parents=True, exist_ok=True)
        df.to_csv(DESTINATION_DIR / "2016" / csv_files[i].relative_to(DATA_DIR), index=False)

# %%
if __name__ == "__main__":
    main()
