import pathlib

# calling in the file names
raw = list(pathlib.Path(".").glob("./data/raw/NREGA_assets_raw/*.csv"))
interim = list(pathlib.Path(".").glob("./data/interim/NREGA_assets/*.csv"))
processed = list(pathlib.Path(".").glob("./data/processed/*.csv"))
lgd = list(pathlib.Path(".").glob("./data/processed/block_lgd_mapped/*.csv"))

folders = [raw, interim, processed, lgd]


#################################  LOWER CASE RENAME ###################################
# renaming the files
for folder in folders:
    for file in folder:
        alpha = file.with_stem(str.lower(file.stem).replace(" ", "_"))
        file = file.rename(alpha)


#################################  UPPER CASE RENAME ###################################
# renaming the files
for folder in folders:
    for file in folder:
        alpha = file.with_stem(str.upper(file.stem).replace("_", " "))
        file = file.rename(alpha)
