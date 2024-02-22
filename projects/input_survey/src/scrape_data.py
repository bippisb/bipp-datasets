import os
import shutil
import time

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager

chrome_options = Options()


def scrape_data():
    try:
        # initialize the webdriver
        driver = webdriver.Chrome(ChromeDriverManager().install())
        driver.implicitly_wait(10)
        driver.maximize_window()

        def redirect_to_table(path, table):
            # click submit button
            submit_btn = driver.find_element(
                By.ID, "_ctl0_ContentPlaceHolder2_cmdSubmit"
            )
            submit_btn.click()
            time.sleep(1.5)

            links = driver.find_elements(By.TAG_NAME, "a")

            file_name = (
                "districttable" + table.split("-")[0].strip().lower()[5:] + ".csv"
            )
            src_path = f"C:/Users/ACLAB/Downloads/{file_name}"
            dest_path = os.path.join(path, file_name)

            for link in links:
                if link.get_attribute("title") == "Export":
                    link.click()
                    links = driver.find_elements(By.TAG_NAME, "a")
                    for link in links:
                        if link.get_attribute("title") == "CSV (comma delimited)":
                            link.click()
                            break
                    break

            time.sleep(2)
            shutil.move(src_path, dest_path)

            # click back button
            back_btn = driver.find_element(By.ID, "btnBack")
            back_btn.click()
            time.sleep(1.5)

        url = "https://inputsurvey.dacnet.nic.in/districttables.aspx"
        driver.get(url)

        select_year = Select(
            driver.find_element(By.ID, "_ctl0_ContentPlaceHolder2_ddlYear")
        )
        select_year.select_by_value("2016")

        root_path = "data/raw"
        n_states = max(0, len(os.listdir(root_path)) - 2)

        select_state = driver.find_element(By.ID, "_ctl0_ContentPlaceHolder2_ddlStates")
        states = select_state.find_elements(By.TAG_NAME, "option")[n_states:]

        state_dict = {}

        for state in states:
            state_dict[state.get_attribute("innerText")] = state.get_attribute("value")

        for state in state_dict.keys():
            state_path = os.path.join(root_path, state)
            os.makedirs(state_path, exist_ok=True)
            n_districts = max(0, len(os.listdir(state_path)) - 1)

            selected_state = Select(
                driver.find_element(By.ID, "_ctl0_ContentPlaceHolder2_ddlStates")
            )
            selected_state.select_by_value(state_dict[state])
            time.sleep(1)
            select_dist = driver.find_element(
                By.ID, "_ctl0_ContentPlaceHolder2_ddldistrict"
            )
            districts = select_dist.find_elements(By.TAG_NAME, "option")[n_districts:]

            district_dict = {}

            for district in districts:
                district_dict[
                    district.get_attribute("innerText")
                ] = district.get_attribute("value")

            for district in district_dict.keys():
                district_path = os.path.join(state_path, district)
                os.makedirs(district_path, exist_ok=True)
                n_tables = max(0, len(os.listdir(district_path)) - 1)

                selected_dist = Select(
                    driver.find_element(By.ID, "_ctl0_ContentPlaceHolder2_ddldistrict")
                )
                selected_dist.select_by_value(district_dict[district])
                time.sleep(1)
                select_table = driver.find_element(
                    By.ID, "_ctl0_ContentPlaceHolder2_ddlTables"
                )
                tables = select_table.find_elements(By.TAG_NAME, "option")[n_tables:]

                table_dict = {}

                for table in tables:
                    table_dict[table.get_attribute("innerText")] = table.get_attribute(
                        "value"
                    )

                for table in table_dict.keys():
                    table_path = os.path.join(district_path, table)
                    os.makedirs(table_path, exist_ok=True)

                    selected_table = Select(
                        driver.find_element(
                            By.ID, "_ctl0_ContentPlaceHolder2_ddlTables"
                        )
                    )
                    selected_table.select_by_value(table_dict[table])
                    time.sleep(1)

                    if table.strip().split("-")[0].lower() == "table4":
                        n_crops = max(0, len(os.listdir(table_path)) - 1)
                        select_crop = driver.find_element(
                            By.ID, "_ctl0_ContentPlaceHolder2_crops"
                        )
                        crops = select_crop.find_elements(By.TAG_NAME, "option")[
                            n_crops:
                        ]

                        crop_dict = {}

                        for crop in crops:
                            crop_dict[
                                crop.get_attribute("innerText")
                            ] = crop.get_attribute("value")

                        for crop in crop_dict.keys():
                            crop_path = os.path.join(table_path, crop)
                            os.makedirs(crop_path, exist_ok=True)

                            selected_crop = Select(
                                driver.find_element(
                                    By.ID, "_ctl0_ContentPlaceHolder2_crops"
                                )
                            )
                            selected_crop.select_by_value(crop_dict[crop])
                            time.sleep(1)
                            redirect_to_table(crop_path, table)

                    elif table.strip().split("-")[0].lower() == "table4a":
                        n_crops = max(0, len(os.listdir(table_path)) - 1)
                        select_crop = driver.find_element(
                            By.ID, "_ctl0_ContentPlaceHolder2_crops1"
                        )
                        crops = select_crop.find_elements(By.TAG_NAME, "option")[
                            n_crops:
                        ]

                        crop_dict = {}

                        for crop in crops:
                            crop_dict[
                                crop.get_attribute("innerText")
                            ] = crop.get_attribute("value")

                        for crop in crop_dict.keys():
                            crop_path = os.path.join(table_path, crop)
                            os.makedirs(crop_path, exist_ok=True)
                            n_fertilizers = max(0, len(os.listdir(crop_path)) - 1)

                            selected_crop = Select(
                                driver.find_element(
                                    By.ID, "_ctl0_ContentPlaceHolder2_crops1"
                                )
                            )
                            selected_crop.select_by_value(crop_dict[crop])
                            time.sleep(1)
                            select_fertilizer = driver.find_element(
                                By.ID, "_ctl0_ContentPlaceHolder2_fertilizers"
                            )
                            fertilizers = select_fertilizer.find_elements(
                                By.TAG_NAME, "option"
                            )[n_fertilizers:]

                            fertilizer_dict = {}

                            for fertilizer in fertilizers:
                                fertilizer_dict[
                                    fertilizer.get_attribute("innerText")
                                ] = fertilizer.get_attribute("value")

                            for fertilizer in fertilizer_dict.keys():
                                fertilizer_path = os.path.join(crop_path, fertilizer)
                                os.makedirs(fertilizer_path, exist_ok=True)

                                selected_fertilizer = Select(
                                    driver.find_element(
                                        By.ID, "_ctl0_ContentPlaceHolder2_fertilizers"
                                    )
                                )
                                selected_fertilizer.select_by_value(
                                    fertilizer_dict[fertilizer]
                                )
                                time.sleep(1)
                                redirect_to_table(fertilizer_path, table)

                    else:
                        redirect_to_table(table_path, table)

    except Exception as e:
        print(e)
        driver.close()
        return True

    driver.close()
    return False


is_scraped = True

while is_scraped:
    is_scraped = scrape_data()
    time.sleep(5)
