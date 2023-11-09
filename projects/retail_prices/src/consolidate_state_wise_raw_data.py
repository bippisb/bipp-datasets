# %%
from pathlib import Path
import pandas as pd

# %%
DATA_DIR = Path(__file__).parents[1] / "data"
RAW_DATA_DIR = DATA_DIR / "raw" / "daily_retail_prices"
OUT_FILE = DATA_DIR / "interim" / "consolidated_state_wise_daily_retail_prices.csv"
INDIA_LEVEL_PARTICULARS = [
    "Average Price",
    "Maximum Price",
    "Minimum Price",
    "Modal Price",
]
COLUMN_ORDER = ["date", "state", "commodity", "price"]
# %%
csv_files = list(RAW_DATA_DIR.glob("*.csv"))

# %%


for i, file in enumerate(csv_files):
    raw_df = pd.read_csv(file)
    df = raw_df.rename(columns={"States/UTs": "state"})

    # add date column
    date = file.stem
    df["date"] = date

    # filter state level data
    cln_df = df[~df["state"].isin(INDIA_LEVEL_PARTICULARS)]

    # convert to long format
    long_df = cln_df.melt(
        id_vars=["state", "date"], var_name="commodity", value_name="price")
    fnl_df = long_df[COLUMN_ORDER]

    # write to csv file
    fnl_df.to_csv(OUT_FILE, mode="a", index=False, header=i == 0)

