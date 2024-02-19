import json
import re
from pathlib import Path
from selenium import webdriver
from selenium.common.exceptions import (NoSuchElementException, TimeoutException, WebDriverException,)
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager
import time
import pandas as pd

dir_path = Path.cwd()
raw_path = Path.joinpath(dir_path, "data", "raw")
interim_path = Path.joinpath(dir_path, "data", "interim")
all_names_path = Path.joinpath(interim_path, "all_names.json")

#calling in the flat list

with open(str(all_names_path), "r") as infile:
    all_names = json.load(infile)

#defining Chrome options

chrome_options = webdriver.ChromeOptions()
prefs = {"download.default_directory": str(dir_path), "profile.default_content_setting_values.automatic_downloads": 1,}
chrome_options.add_experimental_option("prefs", prefs)
chrome_options.add_argument("start-maximized")
chrome_options.add_argument("--ignore-certificate-errors")
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)

#fetching url

url = "https://nrlm.gov.in/shgReport.do?methodName=showMojorityStateWise"
driver.get(url)
driver.implicitly_wait(2)

all_states_file_path = Path.joinpath(raw_path, "all_states.csv")
if not all_states_file_path.exists():
    print ("Scraping all states table...")
    main_table_element = driver.find_element(By.XPATH, '//*[@id="mainex"]')
    main_table_html = main_table_element.get_attribute("outerHTML")
    main_table = pd.read_html(main_table_html, header=3)
    main_table = main_table[0]
    #print(main_table)
    
    main_table.columns= [0,1,2,3,4,5,6,7,8,9,10,11,12,13]
    main_table = main_table.drop([9,10,11,12,13], axis = 1)
    main_table.set_axis(["sno",
                      "state_name",
                      "total_no_of_shg",
                      "sc_shg",
                      "st_shg",
                      "minority_shg",
                      "others_shg",
                      "sub_total",
                      "pwds"],
                     axis=1, inplace=True)
    #removing unnecessary rows
    main_table = main_table[pd.to_numeric(main_table["sno"], errors='coerce').notnull()]
    # #dropping first column
    main_table.drop(columns=main_table.columns[0], axis=1, inplace=True)
    main_table['state_name']= main_table["state_name"].str.title()
    #print(main_table)
    
    #storing all states table as csv
    if not raw_path.exists():
        Path.mkdir(raw_path, parents=True)
    main_table.to_csv(Path.joinpath(raw_path, "all_states.csv"), index=False)
    print ("Scrapped all states table. Proceeding to state level scraping...")

else:
    print ("The csv for all states already exists. Proceeding to state level scraping...")
    
for row in all_names:
    
    state_folder_path = Path.joinpath(raw_path, row["state_name"].lower().strip().replace(" ", "_"))
    state_name_corrected = re.sub(r"[^A-Za-z0-9_]", "", row["state_name"].lower().strip().replace(" ", "_"))
    state_file_path = Path.joinpath(state_folder_path, f"{state_name_corrected}.csv")

    if not state_folder_path.exists():
        state_folder_path.mkdir(parents=True)

    if not state_file_path.exists():
        print("csv for state "+state_name_corrected+" doesn't exist and proceeding for scraping.")
        counter = 0
        while counter<=2:
            try:
                counter+=1
                driver.get(url)
                driver.implicitly_wait(2)
                # selecting the state href web element
                state_page = driver.find_elements(By.XPATH, '//*[@id="mainex"]//a')
                state_names = [x.get_attribute("text") for x in state_page]
                state_hrefs = [x.get_attribute("href") for x in state_page]

                state_dict = {}
                for state, st_href in zip(state_names, state_hrefs):

                    if state == row["state_name"]:
                        state_dict = {state: st_href}
                        
                driver.execute_script(state_dict[row["state_name"]])
                driver.implicitly_wait(1)
                #selecting option to view 100 rows
                no_of_rows = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                no_of_rows.select_by_value("100")

                driver.implicitly_wait(2)

                #retrieving the state table
                state_table_element = driver.find_element(By.XPATH, '//table[@id="example"]')
                state_table_element_html = state_table_element.get_attribute("outerHTML")
                state_table= pd.read_html(state_table_element_html, header=1)
                state_table=state_table[0]
                
                state_table.drop(['Unnamed: 9','Unnamed: 10'], inplace=True, axis=1)

                #print(state_table.columns)
                state_table.set_axis(["sno","district_name","total_no_of_shg","sc_shg","st_shg","minority_shg","others_shg","sub_total","pwds"],axis=1, inplace=True)
                state_table.drop('sno', inplace=True, axis=1)
                state_table.drop(state_table.index[-1], inplace=True, axis=0)
                state_table['district_name'] = state_table['district_name'].str.title()
                state_table.insert(0, 'state_name', row["state_name"].title())
                #print(state_table)
                
                state_table.to_csv(Path.joinpath(state_folder_path, f"{state_name_corrected}.csv"), index=False)
                print ("Scraped " + row["state_name"] + " table. Moving to next scraping task.")
                        
            
            except (NoSuchElementException, TimeoutException, KeyError) as ex:

                print(f"{ex}: Raised at {row['state_name']}")

                print("Calling driver again")

                if counter == 2:
                    break

            except WebDriverException as ex:

                print(f"{ex}: Raised at {row['state_name']}")

                driver.close()

                print("Calling driver again")

                # defining Chrome options
                chrome_options = webdriver.ChromeOptions()
                prefs = {"download.default_directory": str(dir_path), "profile.default_content_setting_values.automatic_downloads": 1,}
                chrome_options.add_experimental_option("prefs", prefs)
                chrome_options.add_argument("start-maximized")

                driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)

    else:
        print("csv for state "+state_name_corrected+ " exists. Moving to next scraping task.")
        
    #scraping district

    district_folder_path = Path.joinpath(state_folder_path, row["district_name"].lower().strip().replace(" ", "_"))
    district_name_corrected = re.sub(r"[^A-Za-z0-9_]", "", row["district_name"].lower().strip().replace(" ", "_"))
    district_file_path = Path.joinpath(district_folder_path, f"{district_name_corrected}.csv")

    if not district_folder_path.exists():
        district_folder_path.mkdir(parents=True)

    if not district_file_path.exists():
        print("csv for district "+district_name_corrected+" doesn't exist and proceeding for scraping.")
        
        counter = 0

        while counter <= 2:
            try:

                counter += 1

                driver.get(url)
                driver.implicitly_wait(2)
                state_page = driver.find_elements(By.XPATH, '//*[@id="mainex"]//a')
                state_names = [x.get_attribute("text") for x in state_page]
                state_hrefs = [x.get_attribute("href") for x in state_page]

                state_dict = {}

                for state, st_href in zip(state_names, state_hrefs):

                    if state == row["state_name"]:
                        state_dict = {state: st_href}
                        
                driver.execute_script(state_dict[row["state_name"]])
                driver.implicitly_wait(1)

                #selecting option to view 100 rows
                no_of_rows = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                no_of_rows.select_by_value("100")

                driver.implicitly_wait(2)
                district_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                district_names = [x.get_attribute("text") for x in district_page]
                district_hrefs = [x.get_attribute("href") for x in district_page]
                
                district_dict={}
                for district, di_href in zip(district_names, district_hrefs):
                    if district == row["district_name"]:
                        district_dict = {district: di_href}

                driver.execute_script(district_dict[row["district_name"]])
                driver.implicitly_wait(2)


                #selecting option to view 100 rows
                no_of_rows = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                no_of_rows.select_by_value("100")

                driver.implicitly_wait(2)

                #retrieving the district table
                district_table_element = driver.find_element(By.XPATH, '//table[@id="example"]')
                district_table_element_html = district_table_element.get_attribute("outerHTML")
                district_table= pd.read_html(district_table_element_html, header=1)
                district_table= district_table[0]
                district_table.drop(district_table.index[-1], inplace=True, axis=0)

                driver.implicitly_wait(2)

                while True:

                    NextButton = driver.find_element(By.XPATH, '//*[@id="example_next"]')
                    next_page_class = NextButton.get_attribute("class")
                    if next_page_class != "paginate_button next disabled":
                        NextButton.click()
                        print("NEXT Button exists for " + district + ". Scraping block names the next page." )

                        driver.implicitly_wait(5)

                        district_table_element_new = driver.find_element(By.XPATH, '//table[@id="example"]')
                        district_table_element_html_new = district_table_element_new.get_attribute("outerHTML")
                        district_table_new= pd.read_html(district_table_element_html_new,header=1)
                        district_table_new=district_table_new[0]
                        district_table_new.drop(district_table.index[-1], inplace=True, axis=0)
                        district_table.append(district_table_new)

                        driver.implicitly_wait(2)

                    else:

                        break
                
                district_table.drop(['Unnamed: 9','Unnamed: 10'], inplace=True, axis=1)

                district_table.set_axis(["sno","block_name","total_no_of_shg","sc_shg","st_shg","minority_shg","others_shg","sub_total","pwds"],axis=1, inplace=True)
                district_table.drop('sno', inplace=True, axis=1)
                
                district_table['block_name'] = district_table['block_name'].str.title()
                district_table.insert(0, 'state_name', row['state_name'].title())
                district_table.insert(1, 'district_name', row['district_name'].title())
                #print(district_table)
                
                #storing district table as csv

                district_table.to_csv(Path.joinpath(district_folder_path, f"{district_name_corrected}.csv"), index=False)

                print ("Scraped " + row['district_name'] + " table. Moving to next scraping task.")
        
            except (NoSuchElementException, TimeoutException, KeyError) as ex:

                print(f"{ex}: Raised at {row['state_name']} {row['district_name']}")

                print("Calling driver again")

                if counter == 2:
                    break

            except WebDriverException as ex:

                print(f"{ex}: Raised at {row['state_name']} {row['district_name']}")

                driver.close()

                print("Calling driver again")
                # defining Chrome options
                chrome_options = webdriver.ChromeOptions()
                prefs = { "download.default_directory": str(dir_path),"profile.default_content_setting_values.automatic_downloads": 1,}
                chrome_options.add_experimental_option("prefs", prefs)
                chrome_options.add_argument("start-maximized")

                driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
    
    
    else:
        print("csv for district "+district_name_corrected+ " exists. Moving to next scraping task.")
    
    #scraping block

    block_folder_path = Path.joinpath(district_folder_path, row["block_name"].lower().strip().replace(" ", "_"))
    block_name_corrected = re.sub(r"[^A-Za-z0-9_]", "", row["block_name"].lower().strip().replace(" ", "_"))
    block_file_path = Path.joinpath(block_folder_path, f"{block_name_corrected}.csv")

    if not block_folder_path.exists():
        block_folder_path.mkdir(parents=True)

    if not block_file_path.exists():
        print("csv for block "+block_name_corrected+" doesn't exist and proceeding for scraping.")

        counter = 0

        while counter <= 2: 
            
            try:
                counter += 1
                driver.get(url)
                driver.implicitly_wait(2)
                state_page = driver.find_elements(By.XPATH, '//*[@id="mainex"]//a')
                state_names = [x.get_attribute("text") for x in state_page]
                state_hrefs = [x.get_attribute("href") for x in state_page]

                state_dict = {}

                for state, st_href in zip(state_names, state_hrefs):

                    if state == row["state_name"]:
                        state_dict = {state: st_href}
                        
                driver.execute_script(state_dict[row["state_name"]])
                driver.implicitly_wait(1)

                #selecting option to view 100 rows
                no_of_rows = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                no_of_rows.select_by_value("100")

                driver.implicitly_wait(2)
                district_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                district_names = [x.get_attribute("text") for x in district_page]
                district_hrefs = [x.get_attribute("href") for x in district_page]
                
                district_dict={}
                for district, di_href in zip(district_names, district_hrefs):
                    if district == row["district_name"]:
                        district_dict = {district: di_href}

                driver.execute_script(district_dict[row["district_name"]])
                driver.implicitly_wait(2)
                
                #selecting option to view 100 rows
                no_of_rows = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                no_of_rows.select_by_value("100")

                driver.implicitly_wait(2)
                block_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                block_names = [x.get_attribute("text") for x in block_page]
                block_hrefs = [x.get_attribute("href") for x in block_page]

                driver.implicitly_wait(2)
                while True:

                    NextButton = driver.find_element(By.XPATH, '//*[@id="example_next"]')
                    next_page_class = NextButton.get_attribute("class")
                    if next_page_class != "paginate_button next disabled":
                        NextButton.click()
                        print("NEXT Button exists for " + district + ". Scraping block names the next page." )

                        driver.implicitly_wait(5)

                        block_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                        block_names_next = [x.get_attribute("text") for x in block_page]
                        block_names.extend(block_names_next)
                        block_hrefs_next = [x.get_attribute("href") for x in block_page]
                        block_hrefs.extend(block_hrefs_next)                                        

                    else:

                        break 
                    
                block_dict ={}
                for block, bl_href in zip(block_names, block_hrefs):
                    if block == row["block_name"]:
                        block_dict = {block: bl_href}
                driver.execute_script(block_dict[row["block_name"]])
                driver.implicitly_wait(2)

                #selecting option to view 100 rows
                no_of_rows = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                no_of_rows.select_by_value("100")

                driver.implicitly_wait(2)

                #retrieving the block table
                block_table_element = driver.find_element(By.XPATH, '//table[@id="example"]')
                block_table_element_html = block_table_element.get_attribute("outerHTML")
                block_table= pd.read_html(block_table_element_html, header=1)
                block_table= block_table[0]
                block_table.drop(block_table.index[-1], inplace=True, axis=0)

                driver.implicitly_wait(2)

                while True:

                    NextButton = driver.find_element(By.XPATH, '//*[@id="example_next"]')
                    next_page_class = NextButton.get_attribute("class")
                    if next_page_class != "paginate_button next disabled":
                        NextButton.click()
                        print("NEXT Button exists for " + block + ". Scraping gp names the next page." )

                        driver.implicitly_wait(5)

                        block_table_element_new = driver.find_element(By.XPATH, '//table[@id="example"]')
                        block_table_element_html_new = block_table_element_new.get_attribute("outerHTML")
                        block_table_new= pd.read_html(block_table_element_html_new, header=1)
                        block_table_new=block_table_new[0]
                        block_table_new.drop(block_table.index[-1], inplace=True, axis=0)
                        block_table.append(block_table_new)

                        driver.implicitly_wait(5)

                    else:

                        break  
                    
                block_table.drop(['Unnamed: 9','Unnamed: 10'], inplace=True, axis=1)
                block_table.set_axis(["sno","grampanchayat_name","total_no_of_shg","sc_shg","st_shg","minority_shg","others_shg","sub_total","pwds"],axis=1, inplace=True)
                block_table.drop('sno', inplace=True, axis=1)
                
                block_table['grampanchayat_name'] = block_table['grampanchayat_name'].str.title()
                block_table.insert(0, 'state_name', row['state_name'].title())
                block_table.insert(1, 'district_name', row['district_name'].title()) 
                block_table.insert(2, 'block_name', row['block_name'].title())
                #print(block_table)   
                
                #storing block table as csv

                block_table.to_csv(Path.joinpath(block_folder_path, f"{block_name_corrected}.csv"), index=False)

                print ("Scraped " + row["block_name"] + " table. Moving to next scraping task.")
                

                
            except (NoSuchElementException, TimeoutException, KeyError) as ex:

                print(f"{ex}: Raised at {row['state_name']} {row['district_name']} {row['block_name']} ")

                print("Calling driver again")

                if counter == 2:
                    break

            except WebDriverException as ex:

                print(f"{ex}: Raised at {row['state_name']} {row['district_name']} {row['block_name']} ")

                driver.close()

                print("Calling driver again")
                # defining Chrome options
                chrome_options = webdriver.ChromeOptions()
                prefs = {"download.default_directory": str(dir_path),"profile.default_content_setting_values.automatic_downloads": 1,}
                chrome_options.add_experimental_option("prefs", prefs)
                chrome_options.add_argument("start-maximized")

                driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)

    else:
       print("csv for block "+block_name_corrected+ " exists. Moving to next scraping task.")
    
    #scraping gp

    gp_folder_path = Path.joinpath(block_folder_path, row["gp_name"].lower().strip().replace(" ", "_"))
    gp_name_corrected = re.sub(r"[^A-Za-z0-9_]", "", row["gp_name"].lower().strip().replace(" ", "_"))
    gp_file_path = Path.joinpath(gp_folder_path, f"{gp_name_corrected}.csv")

    if not gp_folder_path.exists():
        gp_folder_path.mkdir(parents=True)

    if not gp_file_path.exists():
        print("csv for gp "+gp_name_corrected+" doesn't exist and proceeding for scraping.")

        counter = 0

        while counter <= 2:

            try:

                counter += 1
                driver.get(url)
                driver.implicitly_wait(2)
                state_page = driver.find_elements(By.XPATH, '//*[@id="mainex"]//a')
                state_names = [x.get_attribute("text") for x in state_page]
                state_hrefs = [x.get_attribute("href") for x in state_page]

                state_dict = {}

                for state, st_href in zip(state_names, state_hrefs):

                    if state == row["state_name"]:
                        state_dict = {state: st_href}
                        
                driver.execute_script(state_dict[row["state_name"]])
                driver.implicitly_wait(1)

                #selecting option to view 100 rows
                no_of_rows = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                no_of_rows.select_by_value("100")

                driver.implicitly_wait(2)
                district_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                district_names = [x.get_attribute("text") for x in district_page]
                district_hrefs = [x.get_attribute("href") for x in district_page]
                
                district_dict={}
                for district, di_href in zip(district_names, district_hrefs):
                    if district == row["district_name"]:
                        district_dict = {district: di_href}

                driver.execute_script(district_dict[row["district_name"]])
                driver.implicitly_wait(2)
                
                #selecting option to view 100 rows
                no_of_rows = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                no_of_rows.select_by_value("100")

                driver.implicitly_wait(2)
                block_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                block_names = [x.get_attribute("text") for x in block_page]
                block_hrefs = [x.get_attribute("href") for x in block_page]

                driver.implicitly_wait(2)
                while True:

                    NextButton = driver.find_element(By.XPATH, '//*[@id="example_next"]')
                    next_page_class = NextButton.get_attribute("class")
                    if next_page_class != "paginate_button next disabled":
                        NextButton.click()
                        print("NEXT Button exists for " + district + ". Scraping block names the next page." )

                        driver.implicitly_wait(5)

                        block_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                        block_names_next = [x.get_attribute("text") for x in block_page]
                        block_names.extend(block_names_next)
                        block_hrefs_next = [x.get_attribute("href") for x in block_page]
                        block_hrefs.extend(block_hrefs_next)                                        

                    else:

                        break 
                    
                block_dict ={}
                for block, bl_href in zip(block_names, block_hrefs):
                    if block == row["block_name"]:
                        block_dict = {block: bl_href}
                driver.execute_script(block_dict[row["block_name"]])
                driver.implicitly_wait(2)

                #selecting option to view 100 rows
                no_of_rows = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                no_of_rows.select_by_value("100")

                driver.implicitly_wait(2)
                gp_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                gp_names = [x.get_attribute("text") for x in gp_page]
                gp_hrefs = [x.get_attribute("href") for x in gp_page]

                driver.implicitly_wait(2)

                while True:

                    NextButton = driver.find_element(By.XPATH, '//*[@id="example_next"]')
                    next_page_class = NextButton.get_attribute("class")
                    if next_page_class != "paginate_button next disabled":
                        NextButton.click()
                        print("NEXT Button exists for " + block + ". Scraping gp names the next page." )

                        driver.implicitly_wait(2)

                        gp_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                        gp_names_next = [x.get_attribute("text") for x in gp_page]
                        gp_names.extend(gp_names_next)
                        gp_hrefs_next = [x.get_attribute("href") for x in gp_page]
                        gp_hrefs.extend(gp_hrefs_next)
                        driver.implicitly_wait(2)

                    else:

                        break
                gp_dict = {}
                for gp, gp_href in zip(gp_names, gp_hrefs):
                    if gp == row["gp_name"]:
                        
                        gp_dict = {gp: gp_href}
                driver.execute_script(gp_dict[row["gp_name"]])
                driver.implicitly_wait(2)

                #selecting option to view 100 pages
                no_of_rows = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                no_of_rows.select_by_value("100")

                driver.implicitly_wait(2)

                #retrieving the gp table
                gp_table_element = driver.find_element(By.XPATH, '//table[@id="example"]')
                gp_table_element_html = gp_table_element.get_attribute("outerHTML")
                gp_table= pd.read_html(gp_table_element_html,header=1)
                gp_table= gp_table[0]
                gp_table.drop(gp_table.index[-1], inplace=True, axis=0)

                driver.implicitly_wait(2)

                while True:

                    NextButton = driver.find_element(By.XPATH, '//*[@id="example_next"]')
                    next_page_class = NextButton.get_attribute("class")
                    if next_page_class != "paginate_button next disabled":
                        NextButton.click()
                        print("NEXT Button exists for " + gp + ". Scraping village names the next page." )

                        driver.implicitly_wait(2)

                        gp_table_element_new = driver.find_element(By.XPATH, '//table[@id="example"]')
                        gp_table_element_html_new = gp_table_element_new.get_attribute("outerHTML")
                        gp_table_new= pd.read_html(gp_table_element_html_new,header=1)
                        gp_table_new=gp_table_new[0]
                        gp_table_new.drop(gp_table.index[-1], inplace=True, axis=0)
                        gp_table.append(gp_table_new)

                        driver.implicitly_wait(2)

                    else:

                        break  
                    
                gp_table.drop(['Unnamed: 9','Unnamed: 10'], inplace=True, axis=1)
                gp_table.set_axis(["sno","village_name","total_no_of_shg","sc_shg","st_shg","minority_shg","others_shg","sub_total","pwds"],axis=1, inplace=True)
                gp_table.drop('sno', inplace=True, axis=1)
                
                gp_table['village_name'] = gp_table['village_name'].str.title()
                gp_table.insert(0, 'state_name', row["state_name"].title())
                gp_table.insert(1, 'district_name', row["district_name"].title()) 
                gp_table.insert(2, 'block_name', row["block_name"].title())
                gp_table.insert(3, 'grampanchayat_name',row["gp_name"].title())
                #print(gp_table)
                
                #storing gp table as csv

                gp_table.to_csv(Path.joinpath(gp_folder_path, f"{gp_name_corrected}.csv"), index=False)

                print ("Scraped " + row['gp_name'] + " table. Moving to next scraping task.")  
                        
                
            except (NoSuchElementException, TimeoutException, KeyError) as ex:

                print(f"{ex}: Raised at {row['state_name']} {row['district_name']} {row['block_name']} {row['gp_name']} ")

                print("Calling driver again")

                if counter == 2:
                    break

            except WebDriverException as ex:

                print(f"{ex}: Raised at {row['state_name']} {row['district_name']} {row['block_name']} {row['gp_name']} ")

                driver.close()

                print("Calling driver again")
                # defining Chrome options
                chrome_options = webdriver.ChromeOptions()
                prefs = {"download.default_directory": str(dir_path),"profile.default_content_setting_values.automatic_downloads": 1,}
                chrome_options.add_experimental_option("prefs", prefs)
                chrome_options.add_argument("start-maximized")

                driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)

    else:
       print("csv for gp "+gp_name_corrected+ " exists. Moving to next scraping task.")
       
    #scraping village
    village_folder_path = Path.joinpath(gp_folder_path, row["vill_name"].lower().strip().replace(" ", "_"))
    village_name_corrected = re.sub(r"[^A-Za-z0-9_]", "", row["vill_name"].lower().strip().replace(" ", "_"))
    village_file_path = Path.joinpath(village_folder_path, f"{village_name_corrected}.csv")

    if not village_folder_path.exists():
        village_folder_path.mkdir(parents=True)

    if not village_file_path.exists():
        print("csv for village "+village_name_corrected+" doesn't exist and proceeding for scraping.")

        counter = 0

        while counter <= 2:

            try:

                counter += 1
                driver.get(url)
                driver.implicitly_wait(2)
                state_page = driver.find_elements(By.XPATH, '//*[@id="mainex"]//a')
                state_names = [x.get_attribute("text") for x in state_page]
                state_hrefs = [x.get_attribute("href") for x in state_page]

                state_dict = {}

                for state, st_href in zip(state_names, state_hrefs):

                    if state == row["state_name"]:
                        state_dict = {state: st_href}
                        
                driver.execute_script(state_dict[row["state_name"]])
                driver.implicitly_wait(1)

                #selecting option to view 100 rows
                no_of_rows = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                no_of_rows.select_by_value("100")

                driver.implicitly_wait(2)
                district_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                district_names = [x.get_attribute("text") for x in district_page]
                district_hrefs = [x.get_attribute("href") for x in district_page]
                
                district_dict={}
                for district, di_href in zip(district_names, district_hrefs):
                    if district == row["district_name"]:
                        district_dict = {district: di_href}

                driver.execute_script(district_dict[row["district_name"]])
                driver.implicitly_wait(2)
                
                #selecting option to view 100 rows
                no_of_rows = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                no_of_rows.select_by_value("100")

                driver.implicitly_wait(2)
                block_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                block_names = [x.get_attribute("text") for x in block_page]
                block_hrefs = [x.get_attribute("href") for x in block_page]

                driver.implicitly_wait(2)
                while True:

                    NextButton = driver.find_element(By.XPATH, '//*[@id="example_next"]')
                    next_page_class = NextButton.get_attribute("class")
                    if next_page_class != "paginate_button next disabled":
                        NextButton.click()
                        print("NEXT Button exists for " + district + ". Scraping block names the next page." )

                        driver.implicitly_wait(5)

                        block_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                        block_names_next = [x.get_attribute("text") for x in block_page]
                        block_names.extend(block_names_next)
                        block_hrefs_next = [x.get_attribute("href") for x in block_page]
                        block_hrefs.extend(block_hrefs_next)                                        

                    else:

                        break 
                    
                block_dict ={}
                for block, bl_href in zip(block_names, block_hrefs):
                    if block == row["block_name"]:
                        block_dict = {block: bl_href}
                driver.execute_script(block_dict[row["block_name"]])
                driver.implicitly_wait(2)

                #selecting option to view 100 rows
                no_of_rows = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                no_of_rows.select_by_value("100")

                driver.implicitly_wait(2)
                gp_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                gp_names = [x.get_attribute("text") for x in gp_page]
                gp_hrefs = [x.get_attribute("href") for x in gp_page]

                driver.implicitly_wait(2)

                while True:

                    NextButton = driver.find_element(By.XPATH, '//*[@id="example_next"]')
                    next_page_class = NextButton.get_attribute("class")
                    if next_page_class != "paginate_button next disabled":
                        NextButton.click()
                        print("NEXT Button exists for " + block + ". Scraping gp names the next page." )

                        driver.implicitly_wait(2)

                        gp_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                        gp_names_next = [x.get_attribute("text") for x in gp_page]
                        gp_names.extend(gp_names_next)
                        gp_hrefs_next = [x.get_attribute("href") for x in gp_page]
                        gp_hrefs.extend(gp_hrefs_next)
                        driver.implicitly_wait(2)

                    else:

                        break
                gp_dict = {}
                for gp, gp_href in zip(gp_names, gp_hrefs):
                    if gp == row["gp_name"]:
                        
                        gp_dict = {gp: gp_href}
                driver.execute_script(gp_dict[row["gp_name"]])
                driver.implicitly_wait(2)

                #selecting option to view 100 rows
                no_of_rows = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                no_of_rows.select_by_value("100")

                driver.implicitly_wait(2)
                village_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                village_names = [x.get_attribute("text") for x in village_page]
                village_hrefs = [x.get_attribute("href") for x in village_page]

                driver.implicitly_wait(2)

                while True:

                    NextButton = driver.find_element(By.XPATH, '//*[@id="example_next"]')
                    next_page_class = NextButton.get_attribute("class")
                    if next_page_class != "paginate_button next disabled":
                        NextButton.click()
                        print("NEXT Button exists for " + gp + ". Scraping village names the next page." )

                        driver.implicitly_wait(2)

                        village_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                        village_names_next = [x.get_attribute("text") for x in village_page]
                        village_names.extend(gp_names_next)
                        village_hrefs_next = [x.get_attribute("href") for x in village_page]
                        village_hrefs.extend(gp_hrefs_next)

                        driver.implicitly_wait(2)

                    else:

                        break 
                
                for village, vi_href in zip(village_names, village_hrefs):
                    if village == row["vill_name"]:
                        
                        village_dict = {village: vi_href}
                driver.execute_script(village_dict[row["vill_name"]])
                driver.implicitly_wait(2)

                #selecting option to view 100 rows
                no_of_rows = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                no_of_rows.select_by_value("100")

                driver.implicitly_wait(2)

                #retrieving the village table
                village_table_element = driver.find_element(By.XPATH, '//table[@id="example"]')
                village_table_element_html = village_table_element.get_attribute("outerHTML")
                village_table= pd.read_html(village_table_element_html, header=1)
                village_table= village_table[0]
                village_table.drop(village_table.index[-1], inplace=True, axis=0)

                driver.implicitly_wait(2)

                while True:

                    NextButton = driver.find_element(By.XPATH, '//*[@id="example_next"]')
                    next_page_class = NextButton.get_attribute("class")
                    if next_page_class != "paginate_button next disabled":
                        NextButton.click()
                        print("NEXT Button exists for " + village + ". Scraping shg names the next page." )

                        driver.implicitly_wait(2)
                        #village_table.drop(village_table.index[-1], inplace=True, axis=0)

                        village_table_element_new = driver.find_element(By.XPATH, '//table[@id="example"]')
                        village_table_element_html_new = village_table_element_new.get_attribute("outerHTML")
                        village_table_new= pd.read_html(village_table_element_html_new, header=1)
                        village_table_new=village_table_new[0]
                        village_table_new.drop(village_table_new.index[-1], inplace=True, axis=0)
                        #print(village_table_new.columns)
                        village_table=village_table.append(village_table_new)

                        driver.implicitly_wait(2)

                    else:

                        break 
                    
                #print(village_table.columns)
                village_table.set_axis(["sno","group_name","shg_type","sc_shg","st_shg","minority_shg","others_shg","total_member"],axis=1, inplace=True)
                village_table.drop('sno', inplace=True, axis=1)
               
                #village_table= village_table[pd.to_numeric(village_table["sno"], errors='coerce').notnull()]
                village_table['group_name'] = village_table['group_name'].str.title()
                village_table['shg_type'] = village_table['shg_type'].str.title()
                village_table.insert(0, 'state_name', row["state_name"].title())
                village_table.insert(1, 'district_name', row["district_name"].title()) 
                village_table.insert(2, 'block_name', row["block_name"].title())
                village_table.insert(3, 'grampanchayat_name',row["gp_name"].title()) 
                village_table.insert(4, 'village_name', row["vill_name"].title()) 
                    
                #print(village_table)  
                village_table.to_csv(Path.joinpath(village_folder_path, f"{village_name_corrected}.csv"), index=False)

                print ("Scraped " + row["vill_name"] + " table. Moving to next scraping task.")                                                          

                
                
            except (NoSuchElementException, TimeoutException, KeyError) as ex:

                    print(f"{ex}: Raised at {row['state_name']} {row['district_name']} {row['block_name']} {row['gp_name']} {row['vill_name']}")

                    print("Calling driver again")

                    if counter == 2:
                        break

            except WebDriverException as ex:

                print(f"{ex}: Raised at {row['state_name']} {row['district_name']} {row['block_name']} {row['gp_name']} {row['vill_name']}")

                driver.close()

                print("Calling driver again")
                # defining Chrome options
                chrome_options = webdriver.ChromeOptions()
                prefs = {"download.default_directory": str(dir_path),"profile.default_content_setting_values.automatic_downloads": 1,}
                chrome_options.add_experimental_option("prefs", prefs)
                chrome_options.add_argument("start-maximized")

                driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)

    else:
        print("csv for village "+village_name_corrected+ " exists. Moving to next scraping task.")
        
    #scraping shg groups
    group_folder_path = Path.joinpath(village_folder_path, f"{row['group_name'].lower().replace(' ', '_')}_{row['group_id']}")
    group_name_corrected = re.sub(r"[^A-Za-z0-9_]", "", row["group_name"].lower().strip().replace(" ", "_"))
    shg_details_file_path = Path.joinpath(group_folder_path, "shg_details.csv")
    shg_member_details_file_path = Path.joinpath(group_folder_path, "shg_member_details.csv")
    
    if not group_folder_path.exists():
        group_folder_path.mkdir(parents=True)
    
    if not shg_details_file_path.exists() and not shg_member_details_file_path.exists():
        print("csv for group "+group_name_corrected+" doesn't exist and proceeding for scraping.")
        counter = 0

        while counter <= 2:

            try:

                counter += 1
                driver.get(url)
                driver.implicitly_wait(2)
                state_page = driver.find_elements(By.XPATH, '//*[@id="mainex"]//a')
                state_names = [x.get_attribute("text") for x in state_page]
                state_hrefs = [x.get_attribute("href") for x in state_page]

                state_dict = {}

                for state, st_href in zip(state_names, state_hrefs):

                    if state == row["state_name"]:
                        state_dict = {state: st_href}
                        
                driver.execute_script(state_dict[row["state_name"]])
                driver.implicitly_wait(1)

                #selecting option to view 100 rows
                no_of_rows = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                no_of_rows.select_by_value("100")

                driver.implicitly_wait(2)
                district_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                district_names = [x.get_attribute("text") for x in district_page]
                district_hrefs = [x.get_attribute("href") for x in district_page]
                
                district_dict={}
                for district, di_href in zip(district_names, district_hrefs):
                    if district == row["district_name"]:
                        district_dict = {district: di_href}

                driver.execute_script(district_dict[row["district_name"]])
                driver.implicitly_wait(2)
                
                #selecting option to view 100 rows
                no_of_rows = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                no_of_rows.select_by_value("100")

                driver.implicitly_wait(2)
                block_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                block_names = [x.get_attribute("text") for x in block_page]
                block_hrefs = [x.get_attribute("href") for x in block_page]

                driver.implicitly_wait(2)
                while True:

                    NextButton = driver.find_element(By.XPATH, '//*[@id="example_next"]')
                    next_page_class = NextButton.get_attribute("class")
                    if next_page_class != "paginate_button next disabled":
                        NextButton.click()
                        print("NEXT Button exists for " + district + ". Scraping block names the next page." )

                        driver.implicitly_wait(5)

                        block_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                        block_names_next = [x.get_attribute("text") for x in block_page]
                        block_names.extend(block_names_next)
                        block_hrefs_next = [x.get_attribute("href") for x in block_page]
                        block_hrefs.extend(block_hrefs_next)                                        

                    else:

                        break 
                    
                block_dict ={}
                for block, bl_href in zip(block_names, block_hrefs):
                    if block == row["block_name"]:
                        block_dict = {block: bl_href}
                driver.execute_script(block_dict[row["block_name"]])
                driver.implicitly_wait(2)

                #selecting option to view 100 rows
                no_of_rows = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                no_of_rows.select_by_value("100")

                driver.implicitly_wait(2)
                gp_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                gp_names = [x.get_attribute("text") for x in gp_page]
                gp_hrefs = [x.get_attribute("href") for x in gp_page]

                driver.implicitly_wait(2)

                while True:

                    NextButton = driver.find_element(By.XPATH, '//*[@id="example_next"]')
                    next_page_class = NextButton.get_attribute("class")
                    if next_page_class != "paginate_button next disabled":
                        NextButton.click()
                        print("NEXT Button exists for " + block + ". Scraping gp names the next page." )

                        driver.implicitly_wait(2)

                        gp_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                        gp_names_next = [x.get_attribute("text") for x in gp_page]
                        gp_names.extend(gp_names_next)
                        gp_hrefs_next = [x.get_attribute("href") for x in gp_page]
                        gp_hrefs.extend(gp_hrefs_next)
                        driver.implicitly_wait(2)

                    else:

                        break
                gp_dict = {}
                for gp, gp_href in zip(gp_names, gp_hrefs):
                    if gp == row["gp_name"]:
                        
                        gp_dict = {gp: gp_href}
                driver.execute_script(gp_dict[row["gp_name"]])
                driver.implicitly_wait(2)

                #selecting option to view 100 rows
                no_of_rows = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                no_of_rows.select_by_value("100")

                driver.implicitly_wait(2)
                village_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                village_names = [x.get_attribute("text") for x in village_page]
                village_hrefs = [x.get_attribute("href") for x in village_page]

                driver.implicitly_wait(2)

                while True:

                    NextButton = driver.find_element(By.XPATH, '//*[@id="example_next"]')
                    next_page_class = NextButton.get_attribute("class")
                    if next_page_class != "paginate_button next disabled":
                        NextButton.click()
                        print("NEXT Button exists for " + gp + ". Scraping village names the next page." )

                        driver.implicitly_wait(2)

                        village_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                        village_names_next = [x.get_attribute("text") for x in village_page]
                        village_names.extend(gp_names_next)
                        village_hrefs_next = [x.get_attribute("href") for x in village_page]
                        village_hrefs.extend(gp_hrefs_next)

                        driver.implicitly_wait(2)

                    else:

                        break 
                
                for village, vi_href in zip(village_names, village_hrefs):
                    if village == row["vill_name"]:
                        
                        village_dict = {village: vi_href}
                driver.execute_script(village_dict[row["vill_name"]])
                driver.implicitly_wait(2)

                #selecting option to view 100 rows
                no_of_rows = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                no_of_rows.select_by_value("100")

                driver.implicitly_wait(2)
                group_row_select = Select(
                    driver.find_element(
                        By.XPATH, '//*[@id="example_length"]/label/select'
                    )
                )

                group_row_select.select_by_value("100")

                driver.implicitly_wait(2)

                group_table = driver.find_elements(
                    By.XPATH, "//*[@id='example']//a"
                )

                group_hrefs = [x.get_attribute("href") for x in group_table]
                group_names = [x.get_attribute("text") for x in group_table]
                split_hrefs = [href.split(',') for href in group_hrefs]

                # Get the second element in each list (index 1)
                gr_id = [href[1] for href in split_hrefs]

                # Remove the single quotes around each element in gr_id
                gr_id = [item.strip("' '") for item in gr_id]

                """Checking for the NEXT button """

                while True:

                    NextButton = driver.find_element(
                        By.XPATH, '//*[@id="example_next"]'
                    )
                    next_page_class = NextButton.get_attribute("class")

                    if next_page_class != "paginate_button next disabled":

                        NextButton.click()

                        print(
                            f"NEXT Button exists for  {village}. Scraping names from the next page."
                        )

                        driver.implicitly_wait(2)

                        group_table = driver.find_elements(
                            By.XPATH, "//*[@id='example']//a"
                        )

                        group_names_next = [
                            x.get_attribute("text") for x in group_table
                        ]
                        group_hrefs_next = [
                            x.get_attribute("href") for x in group_table
                        ]

                        group_names.extend(group_names_next)
                        group_hrefs.extend(group_hrefs_next)
                        split_hrefs = [href.split(',') for href in group_hrefs_next]

                        # Get the second element in each list (index 1)
                        gr_id_next = [href[1] for href in split_hrefs]

                        # Remove the single quotes around each element in gr_id
                        gr_id_next = [item.strip("' '") for item in gr_id_next]
                        #gr_id_next = [int(x) for x in gr_id_next]

                        # Print the value of gr_id
                        #print(gr_id_next)
                        gr_id.extend(gr_id_next)

                        driver.implicitly_wait(2)

                    else:
                        break
                    
                group_dict ={}
                
                for group, group_href, id_group in zip(group_names, group_hrefs, gr_id):
                    if group == row["group_name"] and id_group == row["group_id"]:
                        group_dict= {group:group_href}
                driver.execute_script(group_dict[row["group_name"]])
                driver.implicitly_wait(2)
                                                                
                
                headings = [th.text.rstrip("\n") for th in driver.find_elements(By.XPATH, '/html/body/div[4]/form/div/div[2]/table/thead/tr/th')[:5] + driver.find_elements(By.XPATH, '/html/body/div[4]/form/div/div[2]/table/thead/tr/th')[6:]]
                data = [(td.text).rstrip("\n") for td in driver.find_elements(By.XPATH, '/html/body/div[4]/form/div/div[2]/table/thead/tr/td')]
                df = pd.DataFrame([data], columns=headings)
                #print(df)
                df.insert(0, 'state_name', row["state_name"].title())
                df.insert(1, 'district_name', row["district_name"].title()) 
                df.insert(2, 'block_name', row["block_name"].title())
                df.insert(3, 'grampanchayat_name',row["gp_name"].title()) 
                df.insert(4, 'village_name', row["vill_name"].title())
                #df.insert(5, 'group_name', group.title()) 
                
                df.columns= [x.lower() for x in df.columns]
                df.columns= [x.replace(" ","_") for x in df.columns]
                df.rename(columns={'shg':'shg_name','self_help_group_id':'shg_id'}, inplace = True)
                df['shg_name']= df['shg_name'].str.title()
                df['bank_name']= df['bank_name'].str.title()
                df['bank_branch_name'] = df['bank_branch_name'].str.title()
                #df = df.applymap(lambda s: s.title() if type(s) == str else s)
                
                #print(df)
                df.to_csv(Path.joinpath(group_folder_path, "shg_details.csv"), index=False)
                print ("Scraped " + row["group_name"] + " shg details table. Moving to next scraping task.")
                
                no_of_rows = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                no_of_rows.select_by_value("100")

                driver.implicitly_wait(2)
                
                shg_members_table = driver.find_element(By.ID, "example")
                #acquiring the table element
                shg_members_table_html = shg_members_table.get_attribute("outerHTML")
                # parsing html via pandas to df
                shg_members = pd.read_html(shg_members_table_html, header=2)
                shg_members=shg_members[0]
                shg_members.drop('Unnamed: 9',inplace=True, axis=1)
                shg_members.columns= [x.lower() for x in shg_members.columns]
                shg_members.columns= [x.replace(" ", "_").replace(".", "") for x in shg_members.columns]
                shg_members.rename(columns={'father/husband_name': 'father_or_husband_name','apl/bpl':'apl_or_bpl'},inplace=True)
                shg_members.drop('sr_no', inplace=True, axis=1)
                #shg_members = shg_members.applymap(lambda s: s.title() if type(s) == str else s)
                shg_members['member_name'] = shg_members['member_name'].str.title()
                shg_members['father_or_husband_name'] = shg_members['father_or_husband_name'].str.title()
                shg_members.insert(0, 'state_name', row["state_name"].title())
                shg_members.insert(1, 'district_name', row["district_name"].title()) 
                shg_members.insert(2, 'block_name', row["block_name"].title())
                shg_members.insert(3, 'grampanchayat_name',row["gp_name"].title()) 
                shg_members.insert(4, 'village_name', row["vill_name"].title())
                shg_members.insert(5, 'group_name', row["group_name"].title()) 
                shg_members.insert(6, 'shg_id', df.loc[0, 'shg_id'])
                
                
                #print(shg_members)
                #storing shg_members table as csv

                shg_members.to_csv(Path.joinpath(group_folder_path, "shg_member_details.csv"), index=False)

                print ("Scraped " + row["group_name"] + " members table. Moving to next scraping task.")
                    
            except (NoSuchElementException, TimeoutException, KeyError) as ex:

                    print(f"{ex}: Raised at {row['state_name']} {row['district_name']} {row['block_name']} {row['gp_name']} {row['vill_name']} {row['group_name']}")

                    print("Calling driver again")

                    if counter == 2:
                        break

            except WebDriverException as ex:

                print(f"{ex}: Raised at {row['state_name']} {row['district_name']} {row['block_name']} {row['gp_name']} {row['vill_name']} {row['group_name']}")

                driver.close()

                print("Calling driver again")
                # defining Chrome options
                chrome_options = webdriver.ChromeOptions()
                prefs = {"download.default_directory": str(dir_path),"profile.default_content_setting_values.automatic_downloads": 1,}
                chrome_options.add_experimental_option("prefs", prefs)
                chrome_options.add_argument("start-maximized")

                driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
        
         
    else:
        print("csv for group "+group_name_corrected+ " exists. Moving to next scraping task.")
    
    
# driver.close()
print("Looping has ended. Scraper rests.")
        