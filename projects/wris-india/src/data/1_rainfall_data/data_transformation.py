# %%
# import libraries

from pathlib import Path

import pandas as pd

# %%
# setting up the path for the directories
project_dir = str(Path(__file__).resolve().parents[3])
raw_folder = project_dir + "/data/raw/"
interim_folder = project_dir + "data/interim"
processed_folder = project_dir + "data/processed"
external_folder = project_dir = "data/external"
# %%
#
pathlist = Path(raw_folder).glob("*/*/*/*.xls")

data = pd.read_excel(
    r"D:\bipp-datasets\projects\wris-india\data\raw\ANDHRA PRADESH\ANANTAPUR\20210101_20210930\20210101_20210930.xls"
)
