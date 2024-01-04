# %%
import pandas as pd
from pathlib import Path
import re
import io

# %%
# Set project, data, and destination directories, and define base columns for data
PROJECT_DIR = Path(__file__).parent.parent
DATA_DIR = PROJECT_DIR / "2016"
DESTINATION_DIR = PROJECT_DIR / "interim"
BASE_COLUMNS = ["survey_year", "table_name", "state_name", "district_name","N","P","K"]
# %%
# Create a list of CSV files in the specified directory and its subdirectories
csv_files = list(DATA_DIR.rglob("**/TABLE4A-*/*/*.csv"))

# %%

# Function to extract pages from a CSV file and return as a list of DataFrames
def extract_pages(p: Path) -> list[pd.DataFrame]:
    # Read the raw text from the file
    with open(p, 'r') as file:
        raw_text = file.read()
    # Replace consecutive newlines with a single newline
    text = re.sub(r'\n{3,}', '\n\n', raw_text)
    # Split the text into pages based on double newlines
    text_pages = list(
        filter(lambda x: x != "", map(str.strip, text.split("\n\n"))))
    assert len(text_pages) == 2, "Table 4A should have 2 pages"
    # Read each page as a DataFrame and return the list of pages
    pages = [pd.read_csv(io.StringIO(p)) for p in text_pages]
    for p in pages:
        assert len(p) == 24, "Each page should have 24 rows"
    return pages[-1:]

# %%

# Function to test the extract_pages function
def test_extract_pages():
    for p in csv_files:
        extract_pages(csv_files[0])


# %%
# Function to rename base columns in a DataFrame
def name_base_columns(df: pd.DataFrame):
    textbox_cols = df.columns[df.columns.str.contains("Textbox")]
    assert len(textbox_cols) == 7, "Table 4 should have 7 textboxes"
    # Rename the first 7 columns to the base columns
    df = df.rename(columns=dict(zip(df.columns[:7], BASE_COLUMNS)))
    return df
# %%


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
        'col15':'SIZE GROUP(HA)', 
        'col16':'NO. OF HOLDINGS_TOTAL NO.', 
        'col17':'NO. OF HOLDINGS_NO. TREATED WITH ONE OR MORE CHEMICAL FERTILIZER', 
        'col18':'AREA UNDER_HYV',
        'col141':'AREA UNDER_HYB',
        'col19':'AREA UNDER_OTHERS',
        'col20':'AREA UNDER_TOTAL',
        'col21':'AREA TREATED WITH ONE OR MORE FERT_HYV',
        'col151':'AREA TREATED WITH ONE OR MORE FERT_HYB',
        'col22':'AREA TREATED WITH ONE OR MORE FERT_OTHERS',
        'col23':'AREA TREATED WITH ONE OR MORE FERT_TOTAL',
        'col24':'TOTAL FERT APPLIED_HYV',
        'col161':'TOTAL FERT APPLIED_HYB',
        'col25':'TOTAL FERT APPLIED_OTHERS',
        'col26':'TOTAL FERT APPLIED_TOTAL',
}
# %%

# Main function that processes and writes the cleaned data to CSV files
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

