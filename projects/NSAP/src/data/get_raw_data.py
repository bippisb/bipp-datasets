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

# Define the data directory
raw_datadir = Path(__file__).parents[1] / "data"
raw_datadir.mkdir(parents=True, exist_ok=True)

# Define global variables
base_url = "https://dashboard.rural.nic.in/dashboardnew/nsap.aspx"
data_extraction_page = base_url

def new_web_driver(raw_datadir: Path = raw_datadir):
    """
    Creates a new instance of the Selenium WebDriver with specific download preferences.

    Parameters:
    - raw_datadir (Path): Path to the directory where downloaded files will be stored.

    Returns:
    - WebDriver: The configured Selenium WebDriver.
    """
    options = webdriver.EdgeOptions()

    options.add_experimental_option("prefs", {
        "download.default_directory": str(raw_datadir.absolute())
    })
    driver = webdriver.Edge(options=options)
    driver.implicitly_wait(1.3)

    return driver

def find_select_element(selector: Tuple[str, str], driver):
    """
    Finds a Select element on a webpage using Selenium.

    Parameters:
    - selector (Tuple[str, str]): Tuple containing the identification method and value.
    - driver: Selenium WebDriver instance.

    Returns:
    - Select: The Select element found on the webpage.
    """
    try:
        select_element = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(selector))
        return Select(select_element)
    except Exception as e:
        print("Couldn't select", selector)
        raise e

def get_options(s: Select):
    """
    Gets options from a Select element.

    Parameters:
    - s (Select): The Select element.

    Returns:
    - List[Tuple]: List of tuples containing (value, text) for each option.
    """
    return [
        (option.get_attribute("value"), option.text)
        for option in s.options
    ]

def handle_alert(driver: webdriver.Chrome, post_accept: Callable = None):
    """
    Handles alert pop-ups on a webpage.

    Parameters:
    - driver (webdriver.Chrome): Selenium WebDriver instance.
    - post_accept (Callable): Optional function to be executed after accepting the alert.

    Returns:
    - bool: True if an alert was handled, False otherwise.
    """
    try:
        alert = driver.switch_to.alert
        print(f"\033[93m Alert: {alert.text}\033[00m")
        alert.accept()
        print("Accepted interrupting alert.")
        if post_accept is not None:
            post_accept()
        return True
    except NoAlertPresentException:
        pass

def build_index() -> List[Dict]:
    """
    Builds an index of selections for data extraction.

    Returns:
    - List[Dict]: List of dictionaries containing the selections for each data extraction point.
    """
    index = []

    # Create a new WebDriver instance
    driver = new_web_driver()
    driver.get(data_extraction_page)
    get_select_element = partial(find_select_element, driver=driver)
    skip_alert = partial(handle_alert, driver=driver)

    # Define selectors for dropdowns
    fin_year_select_id = "ctl00_ContentPlaceHolder1_ddlfin_year"
    state_select_id = "ctl00_ContentPlaceHolder1_ddlstate"
    district_select_id = "ctl00_ContentPlaceHolder1_ddldistrict"
    area_select_id = "ctl00_ContentPlaceHolder1_ddlarea"
    block_select_id = "ctl00_ContentPlaceHolder1_ddlblock"
    grampan_select_id = "ctl00_ContentPlaceHolder1_ddlGP"

    # Select year
    select_year = get_select_element((By.ID, fin_year_select_id))
    years = get_options(select_year)

    for year, year_title in tqdm(years, total=len(years), unit="year", ascii=False, position=0):
        select_year = get_select_element((By.ID, fin_year_select_id))
        select_year.select_by_value(year)
        year_name = year_title
        
        indexed_item = {
            "year": year_name,
            "selections": [
                dict(id=fin_year_select_id, value=year),
            ],
        }

        # Extract state names
        select_state = get_select_element((By.ID, state_select_id))
        states = get_options(select_state)

        for state, state_title in tqdm(states, total=len(states), unit="state",desc=year_title, ascii=False, position=0):

            # select state
            select_state = get_select_element((By.ID, state_select_id))
            select_state.select_by_value(state)
            state_name = state_title

            indexed_item["state"] = state_name
            indexed_item["selections"].append(dict(
                id = state_select_id,
                value = state,
            ))

            skip_alert()

            # extract district names
            select_district = get_select_element((By.ID, district_select_id))
            districts = get_options(select_district)

            for district, district_title in tqdm(districts, total=len(districts), unit="district", desc=state_title, ascii=False, position=0):
                # select district
                select_district = get_select_element((By.ID, district_select_id))
                select_district.select_by_value(district)
                district_name = district_title

                indexed_item["district"] = district_name
                indexed_item["selections"].append(dict(
                    id=district_select_id,
                    value=district,
                ))

                if skip_alert():
                    indexed_item["selections"].pop()
                    continue

                # extract table names
                select_area = get_select_element((By.ID, area_select_id))
                areas = get_options(select_area)

                for area, area_title in tqdm(areas, total=len(areas), unit="area", desc=district_title, ascii=False, position=1):
                    # select table
                    select_area = get_select_element((By.ID, area_select_id))
                    select_area.select_by_value(area)
                    area_name = area_title

                    indexed_item["area"] = area_name
                    indexed_item["selections"].append(dict(
                        id=area_select_id,
                        value=area,
                    ))

                    if skip_alert(post_accept=indexed_item["selections"].pop):
                        continue

                    select_block = get_select_element((By.ID, block_select_id))
                    blocks = get_options(select_block)

                    for block, block_title in tqdm(blocks, total=len(blocks), unit="block", desc=area_title, ascii=False, position=2):
                        # select crop
                        select_block = get_select_element((By.ID, block_select_id))
                        select_block.select_by_value(block)
                        block_name = block_title

                        indexed_item["block"] = block_name
                        indexed_item["selections"].append(dict(
                            id=block_select_id,
                            value=block,
                        ))

                        if skip_alert(post_accept=indexed_item["selections"].pop):
                            continue

                        select_gp = get_select_element((By.ID, grampan_select_id))
                        gps = get_options(select_gp)
                        
                        for gp, gp_title in tqdm(gps, total=len(gps), unit="gp", desc=block_title, ascii=False, position=3):
                            select_gp = get_select_element((By.ID, grampan_select_id))
                            select_gp.select_by_value(gp)
                            gp_name = gp_title

                            indexed_item["gp"] = gp_name
                            indexed_item["selections"].append(dict(
                                id=grampan_select_id,
                                value=gp
                            ))
                            
                            index.append(deepcopy(indexed_item))
                            indexed_item["selections"].pop()  # pop gp
                        indexed_item["selections"].pop()  # pop block
                    indexed_item["selections"].pop()  # pop area     
                indexed_item["selections"].pop()  # pop district
            indexed_item["selections"].pop() # pop state
        indexed_item["selections"].pop() # pop year

    return index
            
    return index


def get_index() -> List[Dict]:
    """
    Retrieves the index from a saved JSON file or builds a new one if not available.

    Returns:
    - List[Dict]: List of dictionaries containing the selections for each data extraction point.
    """
    index_path = raw_datadir / "index.jsonl"
    if index_path.exists():
        # Return existing index
        df = pd.read_json(str(index_path), lines=True)
        return df.to_dict("records")

    # Build a new index
    index = build_index()
    with open(raw_datadir / "index.jsonl", 'w') as json_file:
        json.dump(index, json_file)
    df = pd.read_json(str(index_path), lines=True)
    return df.to_dict("records")


def get_element_value(driver, element_id):
    """
    Retrieves the text value of an element on a webpage.

    Parameters:
    - driver: Selenium WebDriver instance.
    - element_id (str): ID of the HTML element.

    Returns:
    - str: Text value of the element.
    """
    element = driver.find_element(By.ID, element_id)
    return element.text

def scrape_data(index) -> pd.DataFrame:
    """
    Scrapes data from a web page based on the provided index and saves it to a CSV file.

    Parameters:
    - index (List[Dict]): List of dictionaries containing selection information for data extraction.

    Returns:
    - pd.DataFrame: DataFrame containing the scraped data.
    """
    # Create a new WebDriver instance
    driver = new_web_driver()
    driver.get(data_extraction_page)
    get_select_element = partial(find_select_element, driver=driver)
    skip_alert = partial(handle_alert, driver=driver)
    
    # Selectors for different elements on the web page
    fin_year_select_id = "ctl00_ContentPlaceHolder1_ddlfin_year"
    state_select_id = "ctl00_ContentPlaceHolder1_ddlstate"
    district_select_id = "ctl00_ContentPlaceHolder1_ddldistrict"
    area_select_id = "ctl00_ContentPlaceHolder1_ddlarea"
    block_select_id = "ctl00_ContentPlaceHolder1_ddlblock"
    grampan_select_id = "ctl00_ContentPlaceHolder1_ddlGP"
    
    data_list = []

    for i in range(len(index[0])):    
        # Extracting selection values from the index
        year_selection_value = str(index[0][i]["selections"][0]['value'])
        state_selection_value = str(index[0][i]["selections"][1]['value'])
        district_selection_value = str(index[0][i]["selections"][2]['value'])
        area_selection_value = str(index[0][i]["selections"][3]['value'])
        block_selection_value =str(index[0][i]["selections"][4]['value'])
        gp_selection_value = str(index[0][i]["selections"][5]['value'])

        # Setting the selected values in the web page
        select_year = get_select_element((By.ID, fin_year_select_id))
        select_year.select_by_value(year_selection_value)

        select_state = get_select_element((By.ID, state_select_id))
        select_state.select_by_value(state_selection_value)

        select_district = get_select_element((By.ID, district_select_id))
        select_district.select_by_value(district_selection_value)

        select_area = get_select_element((By.ID, area_select_id))
        select_area.select_by_value(area_selection_value)

        select_block = get_select_element((By.ID, block_select_id))
        select_block.select_by_value(block_selection_value)

        select_gp = get_select_element((By.ID, grampan_select_id))
        select_gp.select_by_value(gp_selection_value)

        # Extracting data values from the web page
        totalBeneficiary_value = get_element_value(driver, "ctl00_ContentPlaceHolder1_lblbeneficiary")
        ignoaps_value = get_element_value(driver, "ctl00_ContentPlaceHolder1_lblignoaps")
        igndps_value = get_element_value(driver, "ctl00_ContentPlaceHolder1_lbligndps")
        ignwps_value = get_element_value(driver, "ctl00_ContentPlaceHolder1_lblignwps")
        nfbs_value = get_element_value(driver, "ctl00_ContentPlaceHolder1_lblnfbs")
        bank_account_value = get_element_value(driver, "ctl00_ContentPlaceHolder1_lblbank_mode")
        postal_account_value = get_element_value(driver, "ctl00_ContentPlaceHolder1_lblpo_mode")
        money_order_value = get_element_value(driver, "ctl00_ContentPlaceHolder1_lblmo_mode")
        cash_value = get_element_value(driver, "ctl00_ContentPlaceHolder1_lblcash_mode")
        total_aadhaar_value = get_element_value(driver, "ctl00_ContentPlaceHolder1_lblaadhar")
        verified_aadhaar_value = get_element_value(driver, "ctl00_ContentPlaceHolder1_lblverified_aadhar")
        aadhaar_seeded_value = get_element_value(driver, "ctl00_ContentPlaceHolder1_lblaadhar_seeded")

        # Extracted data is appended to the data list
        year = str(index[0][i]["year"])
        state = str(index[0][i]["state"])
        district = str(index[0][i]["district"])
        area = str(index[0][i]["area"])
        block = str(index[0][i]["block"])
        gp = str(index[0][i]["gp"])

        row_data = [year, state, district, area, block, gp, totalBeneficiary_value, ignoaps_value, igndps_value, ignwps_value, nfbs_value,
                    bank_account_value, postal_account_value, money_order_value, cash_value,
                    total_aadhaar_value, verified_aadhaar_value, aadhaar_seeded_value]
        
        data_list.append(row_data)

    # Creating a DataFrame from the collected data and saving it to a CSV file
    df = pd.DataFrame(data_list, columns=["year", "state", "district", "area", "block", "gp", "Total Beneficiary", "IGNOAPS", "IGNDPS", "IGNWPS", "NFBS", "Bank Account",
                                          "Postal Account", "Money Order", "Cash", "Total Aadhaar", "Verified Aadhaar",
                                          "Aadhaar Seeded"])

    df.to_csv(raw_datadir/'output.csv', index=False)


def separate_districts(input_csv_path, output_dir):
    """
    Separates data in the input CSV file based on year, state, and district, and saves the separated data to specific output directories.

    Parameters:
    - input_csv_path (str): Path to the input CSV file.
    - output_dir (str): Path to the output directory.

    Returns:
    - None
    """
    df = pd.read_csv(input_csv_path)

    # Grouping data based on year, state, and district
    grouped = df.groupby(['year', 'state', 'district'])

    for (year, state, district), group_df in grouped:
        # Creating directory structure for output
        year_state_district_dir = Path(output_dir) / str(year) / state / district
        year_state_district_dir.mkdir(parents=True, exist_ok=True)

        # Saving grouped data to specific output CSV files
        output_file_path = year_state_district_dir / f"{year}_{state}_{district}_output.csv"
        group_df.to_csv(output_file_path, index=False)


def main():

    index = get_index()

    scrape_data(index)

    separate_districts(raw_datadir/"output.csv", raw_datadir.parents[1]/"data"/"raw")
 

if __name__ == "__main__":
    main()
