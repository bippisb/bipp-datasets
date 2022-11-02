import json
from pathlib import Path

"""This scripts flattens the the lists of nested disctionaries and creating a new flat list with  year and month value dictionaries"""


# FLATTENING INTO A SINGLE LIST OF DICTIONARIES
# defining a list for storing all the names of states, district, blocks and GP's
all_names = []

# calling all the json files
time_stamp = "2022_23_September"

dir_path = Path.cwd()
raw_path = Path.joinpath(dir_path, "data", "raw", time_stamp, "jsons")
interim_path = Path.joinpath(dir_path, "data", "interim", time_stamp)

all_names_path = Path.joinpath(interim_path, "all_names.json")

if not interim_path.exists():
    interim_path.mkdir(parents=True)


files = list(raw_path.glob("*/*.json"))


# print(files)

# concatenating all the jsons
for file in files:

    with open(file, "r") as outfile:
        print(file)
        # print(len(file))
        dist_level_names = json.load(outfile)

    all_names.extend(dist_level_names)


# ****************************************************************************

# Sending all  month and year_month as jsons

with open(str(all_names_path), "w") as outfile:
    json.dump(all_names, outfile, ensure_ascii=False)


print(len(all_names))
