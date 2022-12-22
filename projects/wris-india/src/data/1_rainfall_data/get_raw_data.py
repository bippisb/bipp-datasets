# %%
# importing the libraries
# import bs4
# import pandas as pd
# import selenium
import time

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager

driver = webdriver.Chrome(ChromeDriverManager().install())


# def click_dataset(id):
#     a = driver.find_element_by_id(id)


def selenium_scraper(dataset_name, website_url):
    """
    This definition is the main function that is used for scraping wris data.
    """
    # Initiating the link and initializing the webdriver to go to the first webpage
    driver.execute_cdp_cmd(
        "Network.setUserAgentOverride",
        {"userAgent": "python 3.9", "platform": "Any"},
    )
    ## Calling the get_page_data function to obtain data from the web-page
    driver.get(website_url)
    time.sleep(10)
    dataset = driver.find_element_by_xpath(
        '//*[@id="downloadreporthead"]/div/div[2]/div/select'
    )
    dataset.click()

    pass


def main():
    dataset_name = "Rainfall"
    website_url = "https://indiawris.gov.in/wris/#/DataDownload"
    selenium_scraper(dataset_name, website_url)


if __name__ == "__main__":
    main()
