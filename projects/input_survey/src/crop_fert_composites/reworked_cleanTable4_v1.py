# %%
import pandas as pd
from pathlib import Path
import re
import io

# %%
# Define project directories and base columns
PROJECT_DIR = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_DIR / "data"/"raw"
DESTINATION_DIR = PROJECT_DIR / "data"/"interim"

BASE_COLUMNS = ["survey_year", "table_name", "state_name", "district_name"]

# %%
# Create a list of CSV files in the specified directory and its subdirectories
csv_files = list(DATA_DIR.rglob("**/TABLE4-*/*.csv"))

# %%
# Function to extract pages from a CSV file and return as a list of DataFrames
def extract_pages(p: Path) -> list[pd.DataFrame]:
    with open(p, 'r') as file:
        raw_text = file.read()
    # Replace consecutive newlines with a single newline
    text = re.sub(r'\n{3,}', '\n\n', raw_text)
    # Split the text into pages based on double newlines
    text_pages = list(
        filter(lambda x: x != "", map(str.strip, text.split("\n\n"))))
    assert len(text_pages) == 4, "Table 4 should have 4 pages"
    # Read each page as a DataFrame and return the list of pages
    pages = [pd.read_csv(io.StringIO(p)) for p in text_pages]
    for p in pages:
        assert len(p) == 24, "Each page should have 24 rows"
    return pages[-2:]

# %%
# Function to test the extract_pages function
def test_extract_pages():
    for p in csv_files:
        extract_pages(csv_files[0])

# %%
# Function to rename base columns in a DataFrame
def name_base_columns(df: pd.DataFrame):
    textbox_cols = df.columns[df.columns.str.contains("Textbox")]
    assert len(textbox_cols) == 4, "Table 4 should have 4 textboxes"
    # Rename the first 4 columns to the base columns
    df = df.rename(columns=dict(zip(df.columns[:4], BASE_COLUMNS)))
    return df

# %%
# Function to join pages horizontally, keeping only non-base columns
def join_pages(pages: list[pd.DataFrame]):
    # Extract non-base columns from each page and concatenate them horizontally
    num_pages = map(lambda p: p[p.columns.difference(BASE_COLUMNS)], pages)
    wide_df = pd.concat(num_pages, axis=1)
    assert wide_df.shape[0] == 24, "Concatenated dataframe should also have 24 rows, just like the individual pages."
    return wide_df

# %%
# Function to test the join_pages function
def test_join_pages():
    for p in csv_files:
        join_pages(extract_pages(p))

# %%
# Define a mapping for renaming columns
column_name_mapping = {
    'col22': 'SIZE GROUP(HA)',
    'col23': 'NO. OF HOLDINGS_TOTAL NO.',
    'col24': 'NO. OF HOLDINGS_NO. TREATED WITH ONE OR MORE CHEMICAL FERTILIZER',
    'col25': 'AREA UNDER_HYV',
    'col201': 'AREA UNDER_HYB',
    'col26': 'AREA UNDER_OTHERS',
    'col27': 'AREA UNDER_TOTAL',
    'col28': 'AREA TREATED WITH ONE OR MORE FERT_HYV',
    'col211': 'AREA TREATED WITH ONE OR MORE FERT_HYB',
    'col29': 'AREA TREATED WITH ONE OR MORE FERT_OTHERS',
    'col30': 'AREA TREATED WITH ONE OR MORE FERT_TOTAL',
    'col33': 'HYV_N',
    'col34': 'HYV_P',
    'col35': 'HYV_K',
    'col221': 'HYB_N',
    'col231': 'HYB_P',
    'col241': 'HYB_K',
    'col36': 'OTHERS_N',
    'col37': 'OTHERS_P',
    'col38': 'OTHERS_K',
    'col39': 'TOTAL_N',
    'col40': 'TOTAL_P',
    'col192': 'TOTAL_K',
}

# %%
# Main function that processes and writes the cleaned data to CSV files
def main():
    for i in range(len(csv_files)):
        df = join_pages(map(name_base_columns, extract_pages(csv_files[i])))
        df = df.rename(columns=column_name_mapping)
        df = df.drop(columns={'col21', 'col31', 'col32'}, axis=1)
        dest = DESTINATION_DIR / "2016" / csv_files[i].relative_to(DATA_DIR).parent
        dest.mkdir(parents=True, exist_ok=True)
        df.to_csv(DESTINATION_DIR / "2016" / csv_files[i].relative_to(DATA_DIR), index=False)

# %%
if __name__ == "__main__":
    main()