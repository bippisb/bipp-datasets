import logging
import csv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from datetime import datetime
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
import os
from bs4 import BeautifulSoup
import time
import pandas as pd
from selenium.common.exceptions import (
    WebDriverException, 
    NoSuchElementException
)

# Set up logging, replace filename with appropriate name and path
logging.basicConfig(filename=r'C:\Users\Shrav\Dev\isb\web_scraping.log', level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')

# Function to set up the Chrome WebDriver
def setup_driver():
     # Replace executable path to your path where the chrome driver's executable file is present
    chrome_driver_path = r"C:\Users\Shrav\Dev\chromedriver-win64\chromedriver.exe"
    
    if not os.path.exists(chrome_driver_path):
        logging.error(f"ChromeDriver executable not found at {chrome_driver_path}. Please check the path.")
        exit()  
    chrome_options = Options()
    return webdriver.Chrome(service=ChromeService(executable_path=chrome_driver_path), options=chrome_options)
    # Replace executable path to your path where the chrome driver's executable file is present
 

# Function to create a download folder based on the month
def create_download_folder(base_folder, month):
    # Determine the month folder based on the month integer
    month_folder = "August" if month == 8 else "September"
    download_folder = os.path.join(base_folder, month_folder)
    # Create the folder if it doesn't exist
    os.makedirs(download_folder, exist_ok=True)
    return download_folder

# Function to retrieve data from the website
def retrieve_data(driver, url, formatted_date):
    try:
        # Access the website
        driver.get(url)
        # Wait for the website to load completely
        time.sleep(2)
    except WebDriverException as site_error:
        # Handling various WebDriverExceptions
        if "ERR_INTERNET_DISCONNECTED" in str(site_error):   # If connection is lost
            logging.warning("Internet disconnected. Please check your network connection.")
            print("Internet disconnected. Please check your network connection.")
            exit()
        else:
            logging.error(f"Error accessing the website: {site_error}") # If website is not reachable
            print(f"Error accessing the website: {site_error}")
            exit()

    try:
        # Interacting with elements on the webpage (selecting price report, daily prices and date)
        driver.find_element(By.CSS_SELECTOR, '#ctl00_MainContent_Rbl_Rpt_type_0').click() # Select Price Report
        time.sleep(1)
        driver.find_element(By.CSS_SELECTOR, '#ctl00_MainContent_Ddl_Rpt_Option0').click() # Click on the drop down menu
        time.sleep(1)
        driver.find_element(By.CSS_SELECTOR, '#ctl00_MainContent_Ddl_Rpt_Option0 > option:nth-child(2)').click() # Select Daily prices
        driver.find_element(By.CSS_SELECTOR, '#ctl00_MainContent_Txt_FrmDate').send_keys(formatted_date) # Enter the date 
        driver.find_element(By.CSS_SELECTOR, '#ctl00_MainContent_Txt_FrmDate').send_keys(Keys.RETURN) # Press enter key to fetch data for that particular date
    except NoSuchElementException as selenium_error:
        # Handling NoSuchElementException
        logging.error(f"Element not found: {selenium_error}")
        logging.error("The structure of the website may have changed. Please verify.")
        print(f"Element not found: {selenium_error}")
        print("The structure of the website may have changed. Please verify.")

# Function to extract data from the webpage
def extract_data(soup, formatted_date):
    try:

        # Find the table with the specified ID
        table = soup.find('table', {'id': 'gv0'})
        rows = []
        for i, row in enumerate(table.find_all('tr')):
            if i == 0 or i >= 5:
                # Extract data from table cells
                cols = [col.text.strip() for col in row.find_all(['th', 'td'])]
                # Append date and data to the rows list
                rows.append([formatted_date] + cols)
                # Add header for the date column
                rows[0][0] = "Date" 
        return rows
    except (AttributeError, TypeError) as e:
        # Handling AttributeError and TypeError during data extraction for missing data
        logging.error(f"Error occurred during data extraction, data missing: {e}")
        print(f"Error occurred during data extraction, data missing: {e}")

# Function to save data to a CSV file
def save_data(rows, download_folder, formatted_date):
    # Convert date to dd-mm-yyyy format
    formatted_date_for_file = formatted_date.replace("/", "-") 
    # Generate the .csv file name
    new_filename = f"retail_prices_{formatted_date_for_file}.csv"
    try:
        with open(os.path.join(download_folder, new_filename), 'w', encoding='utf-8', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(rows)
    except OSError as file_error:
        logging.error(f"File operation error: {file_error}")
        print(f"File operation error: {file_error}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        print(f"An unexpected error occurred: {e}")

# Function to consolidate data from CSV files
def consolidate_data(base_directory):
    august_folder = os.path.join(base_directory, "August")
    september_folder = os.path.join(base_directory, "September")
    consolidated_data = pd.DataFrame()

    # Loop through csv files for each day in each folder
    for folder in [august_folder, september_folder]:
        for file_name in os.listdir(folder):
            if file_name.endswith(".csv"):
                file_path = os.path.join(folder, file_name)
                df = pd.read_csv(file_path)
                consolidated_data = pd.concat([consolidated_data, df], ignore_index=True)

    # Create path for output file
    output_file = os.path.join(base_directory, "retail_prices_Aug_Sept_2023.csv")

    # Convert consolidated data to csv file
    consolidated_data.to_csv(output_file, index=False)


# Main function to control the overall execution
def main():
    try:
        logging.info('Starting web scraping process')
        chrome_driver = setup_driver()
        url = "https://fcainfoweb.nic.in/reports/report_menu_web.aspx"
        # Replace with your path
        base_download_folder = r"C:\Users\Shrav\Dev\isb"        
        # Define start and end date
        start_date = datetime(2023, 8, 1)
        end_date = datetime(2023, 9, 30)

        for current_date in pd.date_range(start=start_date, end=end_date):
            formatted_date = current_date.strftime("%d/%m/%Y")
            print(formatted_date)

            download_folder = create_download_folder(base_download_folder, current_date.month)
            try:
                retrieve_data(chrome_driver, url, formatted_date)

                page_source = chrome_driver.page_source
                soup = BeautifulSoup(page_source, 'html.parser')

                rows = extract_data(soup, formatted_date)

                save_data(rows, download_folder, formatted_date)
                #add delay between each request to prevemt the website server from overloading
                time.sleep(3)
            except WebDriverException as site_error:
                logging.error(f"Error accessing the website: {site_error}")
        try:
            chrome_driver.find_element(By.CSS_SELECTOR,'#btn_back').click() # Click on the back button
        except NoSuchElementException as selenium_error:
        # Handling NoSuchElementException
            logging.error(f"Element not found: {selenium_error}")
            logging.error("The structure of the website may have changed. Please verify.")
            print(f"Element not found: {selenium_error}")
            print("The structure of the website may have changed. Please verify.")

        consolidate_data(base_download_folder)
        logging.info('Web scraping process completed successfully')

    except Exception as e:
        logging.exception(f"An error occurred in the main function: {e}")
        print(f"An error occurred in the main function: {e}")

# Execute the main function when the script is run directly
if __name__ == "__main__":
    main()
