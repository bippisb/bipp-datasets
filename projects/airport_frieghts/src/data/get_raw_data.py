from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
import time
import requests
import os

options = Options()
options.add_argument("--headless")

url = "https://www.aai.aero/en/business-opportunities/aai-traffic-news"

def process_option(option_value, href_links):
    """
    Process the option value and fetch href links for the given year.
    Args:
        option_value (str): The value of the option to select.
        href_links (list): The list to store the fetched href links.
    Returns:
        None
    Raises:
        Exception: If an error occurs while processing the option.
    """
    driver = webdriver.Firefox(options=options)
    driver.get(url)

    wait = WebDriverWait(driver, 30) 

    try:
        year_dropdown = wait.until(EC.element_to_be_clickable((By.ID, "edit-field-news-date-value-value-year")))

        select = Select(year_dropdown)
        select.select_by_value(option_value)

        submit = wait.until(EC.element_to_be_clickable((By.ID, "edit-submit-aai-traffic-news")))
        submit.click()

        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "views-row-5")))
        time.sleep(5)

        elements = driver.find_elements(By.CLASS_NAME, "views-row-5")
        href_links.extend([element.find_element(By.TAG_NAME, "a").get_attribute("href") for element in elements])
        print(f"Extracted {len(href_links)} href links for year {option_value}")

    except Exception as e:
        print(f"An error occurred for option {option_value}: {e}")
      
    finally:
        driver.quit()

options_dropdown = webdriver.Firefox(options=options)
options_dropdown.get(url)

wait_dropdown = WebDriverWait(options_dropdown, 30)
href_links = []
try:
    year_dropdown = wait_dropdown.until(EC.element_to_be_clickable((By.ID, "edit-field-news-date-value-value-year")))
    year_options = year_dropdown.find_elements(By.TAG_NAME, "option")

    for option in year_options[1:]:
        option_value = option.get_attribute("value")
        process_option(option_value, href_links)

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    options_dropdown.quit()

# Download the files
for href_link in href_links:
    response = requests.get(href_link, verify=False)
    file_name = os.path.join("./data/raw", href_link.split("/")[-1])
    with open(file_name, 'wb') as file:
        file.write(response.content)