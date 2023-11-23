# %%
from pathlib import Path
import polars as pl
import httpx
import json
from core import Globals, API
from core import save_json
# %%


def concat_all_files(items, reader=pl.read_json) -> pl.DataFrame:
    return pl.concat(map(reader, items))


# %%
states = pl.read_json(Globals.SCHOOL_INDEX_DIR.value / "states.json")
districts = concat_all_files(Globals.DISTRICTS_DIR.value.glob("*.json"))
edu_blocks = concat_all_files(Globals.EDU_BLOCKS_DIR.value.glob("*.json"))


# %%
index = districts.drop(["udiseStateCode", "yearId", "isActive", "inityear"]) \
    .join(states.drop(["yearId"]), on="stateId") \
    .join(edu_blocks.drop(["udiseDistrictCode", "yearId", "isActive", "inityear"]), on="districtId")
dest_dir = Globals.SEARCH_SCHOOLS_DIR.value.relative_to(Globals.DATA_DIR.value.parent).as_posix()
index = index.with_columns(
    pl.concat_str([
        pl.lit(dest_dir),
        pl.col("stateId"),
        pl.col("districtId"),
        pl.concat_str([pl.col("eduBlockId"), pl.lit(".json")])
    ], separator="/").alias("search_results_path")
)
index.write_parquet(Globals.SCHOOL_INDEX_DIR.value / "school_search_index.parquet")

# %%
index = index.filter(pl.col("stateName") == "Goa")
index = index.filter(pl.col("search_results_path").map_elements(lambda x: not (Globals.DATA_DIR.value.parent / x).exists()))
print(index.height)

# %%
client = httpx.Client()
# %%
for item in index.iter_rows(named=True):
    r = client.post(
        API.SEARCH_SCHOOL.value,
        data=dict(
            stateName=item["stateId"],
            districtName=item["districtId"],
            blockName=item["eduBlockId"]
        )
    )
    data = r.json()
    dest_path = Globals.DATA_DIR.value.parent / item["search_results_path"]
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    save_json(data, dest_path)
# %%
def read_search_results(path: Path) -> pl.DataFrame:
    data = json.loads(path.read_text())
    return pl.DataFrame(data["list"])

# %%
# %%
schools = pl.concat(map(read_search_results, Globals.SEARCH_SCHOOLS_DIR.value.rglob("*.json")))

# %%
schools.write_parquet(Globals.SCHOOL_INDEX_DIR.value / "schools.parquet")
# %%
