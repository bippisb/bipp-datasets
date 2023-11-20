#!/usr/bin/env python3
# imports
import time

import os 
from time import sleep
import pandas as pd
from bs4 import BeautifulSoup 
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
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import base64

sleep_time=0.2

try:
    os.mkdir('scraped_data')
    os.mkdir("scraped_data/facilities")
except:
    pass
try:
     open("error.txt", 'w').close()
except:
    pass

url="https://src.udiseplus.gov.in/locateSchool/schoolSearch"
# facilities and room details
def main_func():
    
    S=Service(ChromeDriverManager().install())
    chrome_options = Options()
    chrome_options.add_argument('--headless') # dont open the browser
    chrome_options.add_argument('--no-sandbox') # if running as root
    driver = webdriver.Chrome(service=S,options=chrome_options)
    driver.get(url)

    selectElement = driver.find_element(By.ID,"stateName")
    select_state =  Select(selectElement)
    states=[]
    for state in select_state.options:
        states.append(state.text)
    
    states=states[1:2]# andaman only
    for state in states:
        
        select_entries=Select(driver.find_element(By.ID,"example_length").find_element(By.NAME,"example_length"))
        select_entries.select_by_value("1000")
        sleep(0.0001)

        wait = WebDriverWait(driver, 10)
        select_state.select_by_visible_text(state)
        sleep(sleep_time)
        selectElement1 = driver.find_element(By.ID,"districtId") # for districts 
        select_district =  Select(selectElement1)
        districts=[]

        for district in select_district.options:
            districts.append(district.text)
        
        districts=districts[1:]

        if(districts==[]):continue

        for district in districts:
            select_district.select_by_visible_text(district)
            print(district)
            selectElement = driver.find_element(By.ID,"blockId");
            select_block =  Select(selectElement)
            blocks=[]
            sleep(sleep_time)
            for block in select_block.options:
                blocks.append(block.text)
            blocks=blocks[1:]
            for block in blocks:
                select_block.select_by_visible_text(block)
                selectElement = driver.find_element(By.ID,"villageId") # for villages
                select_village =  Select(selectElement)
                villages=[]
                sleep(sleep_time)
                for village in select_village.options:
                    villages.append(village.text)
                villages=villages[1:]


                for village in villages:
                    select_village.select_by_visible_text(village)
                    driver.find_element(By.ID,"searchSchool").find_element(By.TAG_NAME,'input').click() # clicking on school
                    sleep(1)
                    # wait.until(EC.presence_of_element_located((By.XPATH, "//*[@class='collapse' and @class='show']")))
                    table_id = driver.find_element(By.ID, 'example')
                    rows=table_id.find_elements(By.TAG_NAME, "tr") # get all of the rows in the table


                    file_name_fac="scraped_data/facilities/"+state+"_"+district+"_"+block+"_"+village+ ".csv"
                    # f=open(file_name_fac, 'w')
                    # f.close()  
                    headers_fac=["school_name","building_status","boundary_wall","boys_toilet","girls_toilets","cwsn_toilets","drinking_water","hand_wash","functional_generator","library","reading_corner","book_bank","functional_laptops","functional_desktop","functional_tablet","functional_scanner","functional_printer","functional_web_cam","functional_digi_board","internet","class_rooms","others_rooms"]
                    
                    df_fac=pd.DataFrame(columns=headers_fac)
                    original_window = driver.current_window_handle
                    
                    try:
                     for row in rows:  # iterating over schools
                            start=time.time()
                            try:
                                col=row.find_elements(By.TAG_NAME, "td")
                                sleep(sleep_time)
                                if(len(col)==0):continue
                                print(col[2].text)
                                school_name=col[2].text
                                assert len(driver.window_handles) == 1 # changing the driver to new window
                                
                                col[2].find_element(By.TAG_NAME,"a").click()
                            except:
                                continue
                            # sleep(sleep_time)
                            wait.until(EC.number_of_windows_to_be(2))
                            
                            # extracting facilities and room values
                            for window_handle in driver.window_handles:
                                if window_handle != original_window:
                                    driver.switch_to.window(window_handle)
                                    break
                            wait.until(EC.presence_of_element_located((By.ID, "accordion")))
                            get_table=driver.find_element(By.ID,"accordion")
                            fac_room=get_table.find_elements(By.CLASS_NAME,"card-header")
                            fac_room[1].click()
                            sleep(sleep_time)

                            fac_table=driver.find_element(By.ID,"accordion").find_elements(By.CLASS_NAME,"row")
                            facilities_l=[school_name]
                            
                            for i,row in enumerate(fac_table):
                                # if(i%2==0):
                                #     continue
                                x=row.find_elements(By.CLASS_NAME,"col-md-6")
                                y=row.find_elements(By.CLASS_NAME,"col-md-4")
                                for aa in x:
                                    try:
                                        a=aa.find_element(By.CLASS_NAME,"col-5")
                                        try:
                                            facilities_l.append((a.text))
                                        except:
                                            facilities_l.append(a.text)
                                    except:
                                        a=aa.find_element(By.CLASS_NAME,"col-8")
                                        try:
                                            facilities_l.append((a.text))
                                        except:
                                            facilities_l.append(a.text)
                                for aa in y:
                                        try:
                                            a=aa.find_element(By.CLASS_NAME,"col-5")
                                            try:
                                                facilities_l.append((a.text))
                                            except:
                                                facilities_l.append(a.text)
                                        except:
                                            pass
                            fac_room[2].click()
                            sleep(sleep_time)
                            fac_table=driver.find_element(By.ID,"accordion").find_elements(By.CLASS_NAME,"row")
                            for i,row in enumerate(fac_table):
                        
                                x=row.find_elements(By.CLASS_NAME,"col-md-6")
                                for aa in x:
                                    try:
                                        a=aa.find_element(By.CLASS_NAME,"col-7")
                                        try:
                                            facilities_l.append((a.text))
                                        except:
                                            facilities_l.append(a.text)
                                    except:
                                        pass
                            
                            facilities_l = list(filter(None, facilities_l))
                            df_fac.loc[len(df_fac)] = facilities_l
                            
                            # extracting the 4 pdfs using base 64

                            pdfs=driver.find_elements(By.CLASS_NAME,"searchSchoolBtnclass")
                            for pdf in pdfs:
                                try:
                                    pdf.click()
                                    
                                    wait.until(EC.presence_of_element_located((By.XPATH, "//*[@id='myModalReport'][contains(@style, 'display: block')]")))
                                    sleep(sleep_time)
                                    base64string=driver.find_element(By.ID,"ReportCardpdfopenModelURlEmbed").get_attribute("src").split("data:application/pdf;base64,")[1]
                                    school="_".join(school_name.split(" "))
                                    pdf_file="scraped_data/report_card/"+pdf.text+"_"+school+".pdf"

                                    with open(pdf_file, "wb") as f:
                                        f.write(base64.b64decode(base64string))
                                    close_button=driver.find_element(By.ID,"myModalReport").find_element(By.CLASS_NAME,"close")
                                    # print(close_button)
                                    close_button.click()
                                    wait.until(EC.presence_of_element_located((By.XPATH, "//*[@id='myModalReport'][contains(@style, 'display: none')]")))
                                except:
                                    with open("error.txt", "a") as f:
                                        f.write("error in pdf of "+school_name+" in state "+state+" in district "+district +" in block "+block+" in village "+village+"\n")
            
                            driver.close()
                            driver.switch_to.window(original_window)
                            # print(time.time()-start)
                    except:
                        with open("error.txt", "a") as f:
                            f.write("error in "+state+" in district "+district +" in block "+block+" in village "+village+"\n")                    
                    df_fac.to_csv(file_name_fac)
                    
                        
main_func()
