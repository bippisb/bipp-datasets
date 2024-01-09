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
csv_files = list(DATA_DIR.rglob("**/TABLE9A-*.csv"))

# %%


def extract_pages(p: Path) -> list[pd.DataFrame]:
    with open(p, 'r') as file:
        raw_text = file.read()
    text = re.sub(r'\n{3,}', '\n\n', raw_text)
    text_pages = list(
        filter(lambda x: x != "", map(str.strip, text.split("\n\n"))))
    assert len(text_pages) == 4, "Table 9A should have 4 pages"
    pages = [pd.read_csv(io.StringIO(p)) for p in text_pages]
    
    # for page in pages:
    #     # Punjab state data is not complete
    #     # assert len(p) == 6, "Each page should have 6 rows"
    #     if len(page) !=6:
    #         print("NOO",p)
    
    # The correct data required is present in the last two pages. 
    return pages[-2:]

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
        'Col16':'SIZE GROUP(HA)', 
        'Col28':'TotalNumOfOperationalHolding', 
        'Col29':'NoHoldUsingCertSeeds', 
        'Col63':'NoHoldUsingHybridSeeds',
        'Col31':'NoHoldTakenFoundationProg',
        'Col32':'SourceWiseNoHoldersPurchasedCerSEED_DeptOfAgri ',
        'Col33':'SourceWiseNoHoldersPurchasedCerSEED_SeedCorp',
        'Col34':'SourceWiseNoHoldersPurchasedCerSEED_StateAgriUnivFarms',
        'Col35': 'SourceWiseNoHoldersPurchasedCerSEED_COOPFED',
        'Col36':'SourceWiseNoHoldersPurchasedCerSEED_PVTSeedComp',
        'Col37':'SourceWiseNoHoldersPurchasedCerSEED_PVTSeedD/R',
        'Col38':'SourceWiseNoHoldersPurchasedCerSEED_Total',
        'Col41': 'TotalNumOfOperationHolding',
        'Col42': 'NumOfHoldUsingCertSeeds',
        'Col64': 'NumOfHoldUsingHybridSeeds',
        'Col44': 'NumOfHoldTakenFoundationProg',
        'Col45': 'NumOfHoldersEncounteredProblemOfSeedQuality_VarietalImurity',
        'Col46': 'NumOfHoldersEncounteredProblemOfSeedQuality_GerminationImurity',
        'Col47': 'NumOfHoldersEncounteredProblemOfSeedQuality_PhysicalImurity',
        'Col48': 'NumOfHoldersEncounteredProblemOfSeedQuality_InsectDamage',
        'Col49': 'NumOfHoldersEncounteredProblemOfSeedQuality_OtherProb',
        'Col50': 'NumOfHoldersEncounteredProblemOfSeedQuality_Total'
}
# %%


def main():
    for i in range(len(csv_files)):
        print(csv_files[i])
        df = join_pages(map(name_base_columns, extract_pages(csv_files[i])))
        df = df.rename(columns=column_name_mapping)
        # TODO: handle Unnamed columns
        df = df.drop(columns= {'Col15','Col39','Col40'},axis=1)
        dest = DESTINATION_DIR / "2016" / csv_files[i].relative_to(DATA_DIR).parent
        dest.mkdir(parents=True, exist_ok=True)
        df.to_csv(DESTINATION_DIR / "2016" / csv_files[i].relative_to(DATA_DIR), index=False)


if __name__ == "__main__":
    main()

