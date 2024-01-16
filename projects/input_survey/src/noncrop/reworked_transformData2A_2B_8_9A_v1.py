# %%
import pandas as pd
from pathlib import Path
import re
import io

# %%
PROJECT_DIR = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_DIR /"data"/"interim" / "2016"
DESTINATION_DIR = PROJECT_DIR /"data"/"processed"

# %%
csv_files2A = list(DATA_DIR.rglob("**/TABLE2A*.csv"))
csv_files2B = list(DATA_DIR.rglob("**/TABLE2B*.csv"))
csv_files8 = list(DATA_DIR.rglob("**/TABLE8-*.csv"))
csv_files9A = list(DATA_DIR.rglob("**/TABLE9A-*.csv"))

csv_files = csv_files2A + csv_files2B + csv_files8 + csv_files9A


def main():
    for i in range(len(csv_files)):
        print(csv_files[i])
        df = pd.read_csv(csv_files[i])
        dest = DESTINATION_DIR / "2016" / csv_files[i].relative_to(DATA_DIR).parent
        dest.mkdir(parents=True, exist_ok=True)
        df.to_csv(DESTINATION_DIR / "2016" / csv_files[i].relative_to(DATA_DIR), index=False)


if __name__ == "__main__":
    main()
