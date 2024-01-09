from urllib.parse import quote, unquote
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from pathlib import Path
import shutil
import time
from tqdm import tqdm
from selenium.common.exceptions import NoAlertPresentException
from typing import List, Dict, Tuple, Callable
from functools import partial
import json
from copy import deepcopy
from multiprocessing import Pool
import random
import string


# data dir
raw_datadir = Path(__file__).parents[2] / "data" / "raw"
download_dir = raw_datadir / "downloads"
download_dir.mkdir(parents=True, exist_ok=True)


# globals
base_url = "https://inputsurvey.dacnet.nic.in"
data_extraction_page = base_url + "/districttables.aspx"
crop_tables = ["6", "7"]
fertilizer_tables = ["7"]


def quote_name(text: str):
    return quote(text.replace("/", "__or__"))


def unquote_name(text: str):
    return unquote(text.replace("__or__", "/"))


def extract_data(driver, path: Path, download_dir: Path):
    # navigate to data table
    submit_btn = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable(
            (By.ID, "_ctl0_ContentPlaceHolder2_cmdSubmit"))
    )
    submit_btn.click()

    # wait for the report to load
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located(
            (By.ID, "ReportViewer1__ctl9"))
    )

    # add to index
    with open(str(raw_datadir / "index.txt"), "a") as file:
        file.write(str(path) + "\n")

    def download():
        # start file download
        script = "return $find('ReportViewer1').exportReport('CSV');"
        driver.execute_script(script)

    try:
        download()

    except Exception as e:
        time.sleep(5)
        download()

    # wait for the donwload to start
    while len(list(download_dir.glob('*'))) == 0:
        time.sleep(0.2)
    # wait for the download to complete
    file_suffixes = [p.suffix for p in download_dir.glob("*")]
    while ".crdownload" in file_suffixes or ".tmp" in file_suffixes:
        time.sleep(0.5)
        file_suffixes = [p.suffix for p in download_dir.glob("*")]
    time.sleep(0.5)
    # move file to raw data dir
    latest_file = max(download_dir.glob(
        '*'), key=lambda f: f.stat().st_ctime)
    path.parent.mkdir(exist_ok=True, parents=True)

    shutil.move(latest_file, path)

    # # go back

    # back_button = WebDriverWait(driver, 10).until(
    #     EC.element_to_be_clickable((By.ID, "btnBack"))
    # )
    # back_button.click()


def test_downloads():
    num_lines = 0
    with open(raw_datadir / "index.txt", 'r') as file:
        num_lines = sum(1 for line in file)

    folder_path = raw_datadir / "2016"
    file_count = 0

    for file in folder_path.rglob('*'):
        if file.is_file() and file.name != '.DS_Store':
            file_count += 1

    assert file_count == num_lines


def new_web_driver(download_dir: Path = download_dir):
    options = webdriver.EdgeOptions()
    # options.add_argument('--remote-allow-origins=*')
    options.add_experimental_option("prefs", {
        "download.default_directory": str(download_dir.absolute())
    })
    driver = webdriver.Edge(options=options)
    driver.implicitly_wait(1.3)
    # driver.set_network_conditions(
    #     offline=False,
    #     latency=5,
    #     download_throughput=500 * 1024,
    #     upload_throughput=500 * 1024,
    # )
    return driver


def find_select_element(selector: Tuple[str, str], driver):
    try:
        select_element = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(selector))
        return Select(select_element)
    except Exception as e:
        print("Couldn't select", selector)
        raise e


def get_options(s: Select):
    return [
        (option.get_attribute("value"), option.text)
        for option in s.options
    ]


def handle_alert(driver: webdriver.Chrome, post_accept: Callable = None):
    try:
        alert = driver.switch_to.alert
        print(f"\033[93m Alert: {alert.text}\033[00m")
        alert.accept()
        print("Accepted interupting alert.")
        if post_accept is not None:
            post_accept()
        return True
    except NoAlertPresentException:
        pass


def build_index() -> List[Dict]:
    """
    The function should return the list of all scrapable reports.
    This will be later to extract data parallelly, allowing us to
    use multiple webdrivers at a time, to keep record of failures, 
    and to resume from the previous state in case of crashes or failures.

    The index will be a list of dictionaries. Each dictionary should
    contain the following fields.
    - path: Path to which the file will be saved.
    - year: Year
    - state: State Name
    - district: District Name
    - table: Table Name
    - selections: sequence of form inputs (selectors and values)
    """

    index = []

    driver = new_web_driver()
    driver.get(data_extraction_page)
    get_select_element = partial(find_select_element, driver=driver)
    skip_alert = partial(handle_alert, driver=driver)

    # selectors
    year_select_id = "_ctl0_ContentPlaceHolder2_ddlYear"
    state_select_id = "_ctl0_ContentPlaceHolder2_ddlStates"
    district_select_id = "_ctl0_ContentPlaceHolder2_ddldistrict"
    table_select_id = "_ctl0_ContentPlaceHolder2_ddlTables"
    crop_select_id_1 = "_ctl0_ContentPlaceHolder2_crops"
    crop_select_id_2 = crop_select_id_1 + "1"
    fert_select_id = "_ctl0_ContentPlaceHolder2_fertilizers"

    # select year
    year = "2016"
    select_year = get_select_element((By.ID, year_select_id))
    select_year.select_by_value(year)

    # extract state names
    select_state = get_select_element((By.ID, state_select_id))
    states = get_options(select_state)

    for state, state_title in tqdm(states, total=len(states), unit="state", desc="2016-2017", ascii=False, position=0):

        # select state
        select_state = get_select_element((By.ID, state_select_id))
        select_state.select_by_value(state)
        state_name = quote_name(state_title)

        indexed_item = {
            "year": "2016",
            "state": state_title,
            "selections": [
                dict(id=year_select_id, value=year),
                dict(id=state_select_id, value=state)
            ],
        }

        skip_alert()

        # extract district names
        select_district = get_select_element((By.ID, district_select_id))
        districts = get_options(select_district)

        for district, district_title in tqdm(districts, total=len(districts), unit="district", desc=state_title, ascii=False, position=0):

            # select district
            select_district = get_select_element((By.ID, district_select_id))
            select_district.select_by_value(district)
            district_name = quote_name(district_title)

            indexed_item["district"] = district_title
            indexed_item["selections"].append(dict(
                id=district_select_id,
                value=district,
            ))

            if skip_alert():
                indexed_item["selections"].pop()
                continue

            # extract table names
            select_table = get_select_element((By.ID, table_select_id))
            tables = get_options(select_table)

            for table, table_title in tqdm(tables, total=len(tables), unit="table", desc=district_title, ascii=False, position=1):

                # select table
                select_table = get_select_element((By.ID, table_select_id))
                select_table.select_by_value(table)

                indexed_item["table"] = table_title
                indexed_item["selections"].append(dict(
                    id=table_select_id,
                    value=table,
                ))

                if skip_alert(post_accept=indexed_item["selections"].pop):
                    continue

                table_name = quote_name(table_title)
                table_path = raw_datadir / "2016" / state_name / district_name

                if table not in crop_tables:
                    indexed_item["path"] = str(table_path / f"{table_name}.csv")
                    index.append(deepcopy(indexed_item))
                    indexed_item["selections"].pop()  # pop table
                    continue

                crop_table_id = crop_select_id_1 if table != "7" else crop_select_id_2
                select_crop = get_select_element((By.ID, crop_table_id))
                crops = get_options(select_crop)

                for crop, crop_title in tqdm(crops, total=len(crops), unit="crop", desc=table_title.split("-")[0], ascii=False, position=2):

                    # select crop
                    select_crop = get_select_element((By.ID, crop_table_id))
                    select_crop.select_by_value(crop)

                    indexed_item["selections"].append(dict(
                        id=crop_table_id,
                        value=crop,
                    ))

                    if skip_alert(post_accept=indexed_item["selections"].pop):
                        continue

                    crop_name = quote_name(crop_title)
                    crop_path = table_path / table_name

                    if table not in fertilizer_tables:
                        # make a dictionary here to be pushed in the list.
                        indexed_item["path"] = str(crop_path / f"{crop_name}.csv")
                        index.append(deepcopy(indexed_item))
                        indexed_item["selections"].pop()  # pop crop
                        continue

                    # extract fertilizer names
                    select_fert = get_select_element((By.ID, fert_select_id))
                    fertilizers = get_options(select_fert)
                    for fert, fert_title in tqdm(fertilizers, total=len(fertilizers), unit="fertilizer", desc=crop_title, ascii=False, position=3):
                        select_fert = get_select_element(
                            (By.ID, fert_select_id))
                        select_fert.select_by_value(fert)

                        indexed_item["selections"].append(dict(
                            id=fert_select_id,
                            value=fert
                        ))

                        fert_name = quote_name(fert_title + fert)
                        fert_path = crop_path / crop_name
                        indexed_item["path"] = str(fert_path / f"{fert_name}.csv")
                        index.append(deepcopy(indexed_item))
                        indexed_item["selections"].pop()  # pop fertilizer
                    indexed_item["selections"].pop()  # pop crop
                indexed_item["selections"].pop()  # pop table
            indexed_item["selections"].pop()  # pop district

    return index


def get_index() -> List[Dict]:
    """
    Check if the raw data dir contains an already built index.
    Otherwise, build a new index use 'build_index' 
    and save it as a .jsonl file in raw data dir for later use. 
    """
    index_path = raw_datadir / "index.parquet"
    if index_path.exists():
        df = pd.read_parquet(index_path)
        return df.to_dict("records")

    index_path = raw_datadir / "index.jsonl"
    if index_path.exists():
        # return existing index
        df = pd.read_json(str(index_path), lines=True)
        return df.to_dict("records")

    # build new index
    index = build_index()
    df = pd.read_json(json.dumps(index))
    df.to_parquet(str(raw_datadir / "index.parquet"))
    return index


def repr_table(table: dict):
    # logger friendly representation of the item being scraped.
    return " > ".join(
        map(
            unquote_name,
            Path(table["path"]).relative_to(raw_datadir).parts
        )
    )


class Color:
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    END = '\033[0m'


def scrape(table: dict, driver: webdriver.Chrome, download_dir: Path):
    repr = repr_table(table)

    path = Path(table["path"])
    if path.is_file():
        print(f"EXISTS {repr}")
        return None

    try:
        print(Color.OKBLUE, f"GET {repr}", Color.END)
        driver.get(data_extraction_page)
        get_select_input = partial(find_select_element, driver=driver)

        for selection in table["selections"]:
            id = selection["id"]
            val = selection["value"]
            select = get_select_input((By.ID, id))
            select.select_by_value(val)

        extract_data(path=path, driver=driver, download_dir=download_dir)
        print(Color.OKGREEN, f"SUCCESS {repr}", Color.END)
        return True
    except Exception as e:
        print(Color.FAIL, f"FAILED {repr}", Color.END)
        print(e)
        return False


def take_n(items: list, n: int = 100):
    start = 0
    for end in range(n, len(items) + 1, n):
        yield items[start:end]
        start = end


def randomword(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))


def work(tables):
    uniq_down_dir = download_dir / randomword(5)
    uniq_down_dir.mkdir(parents=True, exist_ok=True)
    driver = new_web_driver(
        download_dir=uniq_down_dir
    )
    driver.get(data_extraction_page)
    out = list(
        map(partial(scrape, driver=driver, download_dir=uniq_down_dir), tables))
    uniq_down_dir.rmdir()
    driver.quit()
    return out


def main():
    index = get_index()
    print(len(index), "items to scrape.")

    with Pool(2) as p:
        p.map(work, take_n(index))


if __name__ == "__main__":
    main()
