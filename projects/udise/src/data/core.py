from enum import Enum
from pathlib import Path
import json

Path.cwd().as_posix()

class Globals(Enum):
    DATA_DIR = Path(__file__).parents[2] / "data"
    RAW_DATA_DIR = DATA_DIR / "raw"
    SCHOOL_INDEX_DIR = RAW_DATA_DIR / "school_index"
    SEARCH_SCHOOLS_DIR = SCHOOL_INDEX_DIR / "schools"
    BASE_URL = "https://src.udiseplus.gov.in"
    DISTRICTS_DIR = SCHOOL_INDEX_DIR / "districts"
    EDU_BLOCKS_DIR = SCHOOL_INDEX_DIR / "edu_blocks"
    CLUSTERS_DIR = SCHOOL_INDEX_DIR / "clusters"
    VILLAGES_DIR = SCHOOL_INDEX_DIR / "villages"
    SCHOOLS_DIR = RAW_DATA_DIR / "schools"


Globals.SCHOOL_INDEX_DIR.value.mkdir(exist_ok=True)
Globals.DISTRICTS_DIR.value.mkdir(exist_ok=True)
Globals.EDU_BLOCKS_DIR.value.mkdir(exist_ok=True)
Globals.CLUSTERS_DIR.value.mkdir(exist_ok=True)
Globals.VILLAGES_DIR.value.mkdir(exist_ok=True)
Globals.SEARCH_SCHOOLS_DIR.value.mkdir(exist_ok=True)


class API(Enum):
    BASE_URL = Globals.BASE_URL.value

    # list of states (GET)
    STATE_LIST = BASE_URL + "/state"

    # list of districts (POST - json: stateId)
    DISTRICT_LIST = BASE_URL + "/locateSchool/getDistrict"

    # list of education blocks (GET - param: districtId)
    EDU_BLOCK_LIST = BASE_URL + "/locateSchool/getBlock"

    # list of clusters (GET - path: eduBlockId)
    CLUSTER_LIST = BASE_URL + "/locateSchool/cluster"

    # list of villages (GET - path: eduBlockId)
    VILLAGE_LIST = BASE_URL + "/locateSchool/village"

    # searching schools
    # (POST - formdata: {stateName, districtName, blockName, villageId, Search[search]})
    # Note: Although the keys are suffixed with "Name", they take IDs as values.
    SEARCH_SCHOOL = BASE_URL + "/locateSchool/searchSchool"

    # school detail
    # (POST - formdata: schoolIdforDashSearch)
    SCHOOL_DETAIL = BASE_URL + "/searchSchool/getSchoolDetail"

    # track school (POST - formdata: schoolId)
    TRACK_SCHOOL = BASE_URL + "/searchSchool/trackSchool"

    # school report card
    # (POST - formdata: {schoolId, yearId})
    REPORT_CARD = BASE_URL + "/NewReportCard/PdfReportSchId"


def save_json(data, path):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
