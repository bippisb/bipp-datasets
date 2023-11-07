#!/usr/bin/env python3
# imports
import os 
from time import sleep
import pandas as pd
from bs4 import BeautifulSoup # 4.9.3
import regex as re
import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.remote.command import Command

# defining a global sleep time to sleep after a page is loaded
sleep_time=0.00001

# creating a folder to store the scraped data
try:
    os.mkdir('scraped_data')
except:
    pass
try:
     open("error.txt", 'w').close()
except:
    pass
# function to remove non english characters from the headers 
def remove_non_english(lst):
    return re.sub(r'[^A-Za-z0-9 ]+', '', lst)


    
def check_driver(driver,date):    
    status=driver.service.is_connectable()
    if(status==False):
            print("Driver Dead")
            driver.quit()
            with open("error.txt", "a") as myfile:
                myfile.write("Error in driver for date: "+str(date.strftime("%d/%m/%Y"))+"\n")
                print("hi")
                return 0
    return 1
def scraper_func(date):

# defining the driver and the url

    print("Opening driver for date: ",date.strftime("%d/%m/%Y"))
    S=Service(ChromeDriverManager().install())
    chrome_options = Options()
    chrome_options.add_argument('--headless') # dont open the browser
    chrome_options.add_argument('--no-sandbox') # if running as root
    driver = webdriver.Chrome(service=S,options=chrome_options)

# Error handling if the driver is not working
    if(check_driver(driver,date)==0):
        return
    driver.get("https://fcainfoweb.nic.in/reports/report_menu_web.aspx")
    if(check_driver(driver,date)==0):
        return 
    
# selecting the prices report option by id

    try:
        radio = driver.find_element(By.ID,"ctl00_MainContent_Rbl_Rpt_type_0")
        radio.click()
        sleep(sleep_time)
    except:

        with open("error.txt", "a") as myfile:
                myfile.write("Error in driver/radio button for price report: "+str(date.strftime("%d/%m/%Y"))+"\n")
        return
# selecting the daily prices option by id
    try:
        objSelect = Select(driver.find_element(By.ID,"ctl00_MainContent_Ddl_Rpt_Option0"))
        objSelect.select_by_visible_text("Daily Prices")
        sleep(sleep_time)
    except:
            with open("error.txt", "a") as myfile:
                    myfile.write("Error in dropdown for daily price: "+str(date.strftime("%d/%m/%Y"))+"\n")
            return

# sending the date 
    try:
        date_scrape=driver.find_element(By.ID,"ctl00_MainContent_Txt_FrmDate")
        date_scrape.send_keys(str(date.strftime("%d/%m/%Y"))) # format dd/mm/yyyy
        sleep(sleep_time)
    except:
        with open("error.txt", "a") as myfile:
                myfile.write("Error in sending date for date: "+str(date.strftime("%d/%m/%Y"))+"\n")
        return
# clicking the submit button
    try:
        submit = driver.find_element(By.ID,"ctl00_MainContent_btn_getdata1")
        submit.click()
        sleep(sleep_time)
    except:
        with open("error.txt", "a") as myfile:
                myfile.write("Error in submit button for date: "+str(date.strftime("%d/%m/%Y"))+"\n")
        return
# scraping the page using beautifulsoup
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    table = soup.find('table', {'id': 'gv0'})

# getting all the data from the table
    print("Data Extracted")
    data = []
    for row in table.find_all('tr'):
        row_data = []
        for cell in row.find_all('td'):
            row_data.append( cell.text)
        data.append(row_data)

# getting the headers for the table
    extracted_head=table.find_all('th')
    headers=[]
    for head in extracted_head:
        headers.append(remove_non_english(head.text)) # removing non english characters from the headers

# remove the first 5 rows as they are not required    
    df = pd.DataFrame(data).iloc[5:]

# storing in a file
    file_name="scraped_data/retail_prices_"+str(date.strftime("%b"))+"_"+str(date.strftime("%d"))+"_"+str(str(date.strftime("%Y")))+".csv"
    df.to_csv(file_name, header=headers, index=False)

# closing the driver
    print("driver closed for date: ",date.strftime("%d/%m/%Y"))
    driver.close()

if __name__ == '__main__':

    # defining the start and end date
    start_date = datetime.date(2023, 8, 1)
    end_date = datetime.date(2023, 9, 30)
    delta = datetime.timedelta(days=1)
    
    # looping over all the dates
    while (start_date <= end_date):
        file_name="retail_prices_"+str(start_date.strftime("%b"))+"_"+str(start_date.strftime("%d"))+"_"+str(str(start_date.strftime("%Y")))+".csv"
        scraper_func(start_date)
        start_date += delta

