import json
from pathlib import Path

from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager

dir_path = Path.cwd()
raw_path = Path.joinpath(dir_path, "data", "raw", "jsons")
interim_path = Path.joinpath(dir_path, "data", "interim")
external_path = Path.joinpath(dir_path, "data", "external")

"""Scraping Begins Here"""


# defining Chrome options
chrome_options = webdriver.ChromeOptions()
prefs = {
    "download.default_directory": str(dir_path),
    "profile.default_content_setting_values.automatic_downloads": 1,
}
chrome_options.add_experimental_option("prefs", prefs)
chrome_options.add_argument("start-maximized")
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])

driver = webdriver.Chrome(
    ChromeDriverManager().install(), options=chrome_options
)

#fetching the url
url = "https://nrlm.gov.in/shgReport.do?methodName=showMojorityStateWise"
driver.get(url)
driver.implicitly_wait(2)

state_page = driver.find_elements(
    By.XPATH, "//*[@class='panel panel-default']//a"
)
state_page = state_page[2:]

state_names = [x.get_attribute("text") for x in state_page]
state_hrefs = [x.get_attribute("href") for x in state_page]

for state, st_href in zip(state_names, state_hrefs):

    driver.execute_script(st_href)

    driver.implicitly_wait(2)

    district_row_select = Select(
        driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select')
    )

    district_row_select.select_by_value("100")

    driver.implicitly_wait(2)
    
    district_table = driver.find_elements(By.XPATH, "//*[@id='example']//a")

    district_names = [x.get_attribute("text") for x in district_table]
    district_hrefs = [x.get_attribute("href") for x in district_table]
    
    for district, dist_href in zip(district_names, district_hrefs):
        
        try:
            district_list=[]
            
            state_path = Path.joinpath(raw_path, state.lower().replace(" ", "_"))
            json_name = ".".join([district.lower().replace(" ", "_"), "json"])
            district_json_path = Path.joinpath(raw_path, state.lower().replace(" ", "_"), json_name)

            if not state_path.exists():
                Path.mkdir(state_path, parents=True)

            if not district_json_path.exists():
                print(
                    district_json_path,
                    "doesn't exist and proceeding for scraping",
                )

                district_row_select = Select(
                    driver.find_element(
                        By.XPATH, '//*[@id="example_length"]/label/select'
                    )
                )

                district_row_select.select_by_value("100")

                driver.execute_script(dist_href)

                driver.implicitly_wait(2)

                block_row_select = Select(
                    driver.find_element(
                        By.XPATH, '//*[@id="example_length"]/label/select'
                    )
                )

                block_row_select.select_by_value("100")

                driver.implicitly_wait(2)

                block_table = driver.find_elements(
                    By.XPATH, "//*[@id='example']//a"
                )

                block_names = [x.get_attribute("text") for x in block_table]

                block_hrefs = [x.get_attribute("href") for x in block_table]
                """Checking for the NEXT button in block list of district page"""
                
                while True:

                    NextButton = driver.find_element(
                        By.XPATH, '//*[@id="example_next"]'
                    )
                    next_page_class = NextButton.get_attribute("class")

                    if next_page_class != "paginate_button next disabled":

                        NextButton.click()

                        print(
                            f"NEXT Button exists for {state} {district}. Scraping the next page."
                        )

                        driver.implicitly_wait(2)

                        block_table = driver.find_elements(
                            By.XPATH, "//*[@id='example']//a"
                        )

                        block_names_next = [
                            x.get_attribute("text") for x in block_table
                        ]

                        block_names.extend(block_names_next)

                        block_hrefs_next = [
                            x.get_attribute("href") for x in block_table
                        ]

                        block_hrefs.extend(block_hrefs_next)

                        # driver.implicitly_wait(10)

                    else:
                        break
                    
                for block, block_href in zip(block_names, block_hrefs):

                    while True:

                        try:

                            print(
                                f"Attempting scrape of {state} {district} {block}"
                            )

                            block_row_select = Select(
                                driver.find_element(
                                    By.XPATH,
                                    '//*[@id="example_length"]/label/select'
                                )
                            )

                            block_row_select.select_by_value("100")

                            driver.implicitly_wait(2)

                            driver.execute_script(block_href)

                            driver.implicitly_wait(2)

                            gp_row_select = Select(
                                driver.find_element(
                                    By.XPATH,
                                    '//*[@id="example_length"]/label/select'
                                )
                            )

                            gp_row_select.select_by_value("100")

                            driver.implicitly_wait(2)

                            gp_table = driver.find_elements(
                                By.XPATH, "//*[@id='example']//a"
                            )
                            
                            gp_names = [
                                x.get_attribute("text") for x in gp_table
                            ]
                            
                            gp_hrefs = [x.get_attribute("href") for x in gp_table]
                            
                            """Checking for the NEXT button in GP list of block page"""
                            
                            while True:

                                NextButton = driver.find_element(
                                    By.XPATH, '//*[@id="example_next"]'
                                )
                                next_page_class = NextButton.get_attribute(
                                    "class"
                                )

                                if (
                                    next_page_class
                                    != "paginate_button next disabled"
                                ):

                                    NextButton.click()

                                    print(
                                        f"NEXT Button exists for {state} {district} {block}. Scraping the next page."
                                    )

                                    driver.implicitly_wait(2)

                                    gp_table = driver.find_elements(
                                        By.XPATH, "//*[@id='example']//a"
                                    )
                                    
                                    gp_names_next = [
                                        x.get_attribute("text")
                                        for x in gp_table
                                    ]

                                    gp_names.extend(gp_names_next)
                                    gp_hrefs_next = [
                                        x.get_attribute("href")
                                        for x in gp_table
                                    ]
                                    
                                    gp_hrefs.extend(gp_hrefs_next)

                                    driver.implicitly_wait(2)

                                else:
                                    break
                            
                            
                            for gp, gp_href in zip(gp_names, gp_hrefs):
                                
                                while True:
                                    
                                    try:
                                        print(
                                            f"Attempting scrape of {state} {district} {block} {gp}"
                                        )
                                        
                                        gp_row_select = Select(
                                            driver.find_element(
                                                By.XPATH,
                                                '//*[@id="example_length"]/label/select'
                                            )
                                        )
                                        
                                        gp_row_select.select_by_value("100")
                                        driver.implicitly_wait(2)
                                        
                                        driver.execute_script(gp_href)
                                        driver.implicitly_wait(2)
                                        
                                        vill_row_select = Select(
                                            driver.find_element(
                                                By.XPATH,
                                                '//*[@id="example_length"]/label/select'
                                            )
                                        )
                                        vill_row_select.select_by_value("100")
                                        driver.implicitly_wait(2)
                                        
                                        vill_table = driver.find_elements(
                                            By.XPATH, "//*[@id='example']//a"
                                        )
                                        
                                        vill_names = [
                                            x.get_attribute("text") for x in vill_table
                                        ]
                                        
                                        vill_hrefs = [
                                            x.get_attribute("href") for x in vill_table
                                        ]
                                        
                                        while True:
                                            
                                            NextButton = driver.find_element(
                                                By.XPATH, '//*[@id="example_next"]'
                                            )
                                            next_page_class = NextButton.get_attribute("class")

                                            if next_page_class != "paginate_button next disabled":

                                                NextButton.click()

                                                print(
                                                    f"NEXT Button exists for {state} {district} {block} {gp}. Scraping the next page."
                                                )

                                                driver.implicitly_wait(2)
                                                
                                                vill_table = driver.find_elements(
                                                    By.XPATH, "//*[@id='example']//a"
                                                )
                                                
                                                vill_names_next = [
                                                    x.get_attribute("text")
                                                    for x in vill_table
                                                ]

                                                vill_names.extend(vill_names_next)
                                                vill_hrefs_next = [
                                                    x.get_attribute("href")
                                                    for x in vill_table
                                                ]
                                                
                                                vill_hrefs.extend(vill_hrefs_next)

                                                driver.implicitly_wait(2)

                                            else:
                                                break 
                                            
                                        for vill, vill_href in zip(vill_names, vill_hrefs):
                                            
                                            while True:
                                                
                                                try:
                                                    print(
                                                    f"Attempting scrape of {state} {district} {block} {gp} {vill}"
                                                    )
                                                    
                                                    vill_row_select = Select(
                                                        driver.find_element(
                                                            By.XPATH,
                                                            '//*[@id="example_length"]/label/select'
                                                        )
                                                    )
                                                    vill_row_select.select_by_value("100")
                                                    driver.implicitly_wait(2)
                                                    driver.execute_script(vill_href)
                                                    driver.implicitly_wait(2)
                                                    
                                                    group_row_select = Select(
                                                        driver.find_element(
                                                            By.XPATH,
                                                            '//*[@id="example_length"]/label/select'
                                                        )
                                                    )
                                                    
                                                    group_row_select.select_by_value("100")
                                                    driver.implicitly_wait(2)
                                                    group_table = driver.find_elements(
                                                        By.XPATH, "//*[@id='example']//a"
                                                    )
                                                    group_names = [
                                                        x.get_attribute("text") for x in group_table
                                                    ]
                                                    group_hrefs = [
                                                        x.get_attribute("href") for x in group_table
                                                    ]
                                                    
                                                    split_hrefs = [href.split(',') for href in group_hrefs]

                                                    # Get the second element in each list (index 1)
                                                    gr_id = [href[1] for href in split_hrefs]

                                                    # Remove the single quotes around each element in gr_id
                                                    gr_id = [item.strip("' '") for item in gr_id]
                                                    #gr_id = [int(x) for x in gr_id]

                                                    # Print the value of gr_idS
                                                    #print(gr_id)
                                                    
                                                    while True:
                                            
                                                        NextButton = driver.find_element(
                                                            By.XPATH, '//*[@id="example_next"]'
                                                        )
                                                        next_page_class = NextButton.get_attribute("class")

                                                        if next_page_class != "paginate_button next disabled":

                                                            NextButton.click()

                                                            print(
                                                                f"NEXT Button exists for {state} {district} {block} {gp} {vill}. Scraping the next page."
                                                            )

                                                            driver.implicitly_wait(2)
                                                            
                                                            group_table = driver.find_elements(
                                                                By.XPATH, "//*[@id='example']//a"
                                                            )
                                                            
                                                            group_names_next = [
                                                                x.get_attribute("text")
                                                                for x in group_table
                                                            ]

                                                            group_names.extend(group_names_next)
                                                            group_hrefs_next = [
                                                                x.get_attribute("href")
                                                                for x in group_table
                                                            ]
                                                            
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
                                                            #print(gr_id)
  
                                                            driver.implicitly_wait(2)

                                                        else:
                                                            break
                                                        
                                                    for i,g in enumerate(group_names):
                                                        #name_split= [ i for i in g]
                                                        
                                                        if str.isascii(g):
                                                            vill_dict ={}
                                                            
                                                            vill_dict = {
                                                                "state_name": state,
                                                                "district_name": district,
                                                                "block_name": block,
                                                                "gp_name": gp,
                                                                "vill_name": vill,
                                                                "group_name": g,
                                                                "group_id": gr_id[i]
                                                                
                                                            }
                                                            # for key, value in vill_dict.items():
                                                            #     vill_dict[key] = value.lower().replace(" ", "_")
                                                            district_list.append(vill_dict)
                                                        
                                                            
                                                        else:
                                                            pass
                                                    #print(district_list)
                                                    
                                                    BackButton = driver.find_element(
                                                        By.XPATH,
                                                        '//*[@id="panelfilter"]/ul/li[2]/div/input[2]',
                                                    ).click()

                                                    break
                                                        
                             
                                                except NoSuchElementException:
                                                    print(
                                                        f"No Such Element raised at {state} {district} {block} {gp} {vill}"
                                                    )

                                                    driver.get(url)
                                                    driver.implicitly_wait(2)

                                                    driver.execute_script(st_href)

                                                    driver.implicitly_wait(2)

                                                    district_row_select = Select(
                                                        driver.find_element(
                                                            By.XPATH,
                                                            '//*[@id="example_length"]/label/select',
                                                        )
                                                    )

                                                    district_row_select.select_by_value("100")

                                                    driver.implicitly_wait(2)

                                                    driver.execute_script(dist_href)
                                                    
                                                    driver.execute_script(block_href)
                                                    driver.execute_script(gp_href)

                                                    driver.implicitly_wait(2)
                                                    
                                        BackButton = driver.find_element(
                                            By.XPATH,
                                            '//*[@id="panelfilter"]/ul/li[2]/div/input[2]',
                                        ).click()

                                        break           
                                        
                                    except NoSuchElementException:
                                        print(
                                            f"No Such Element raised at {state} {district} {block} {gp}"
                                        )

                                        driver.get(url)
                                        driver.implicitly_wait(2)

                                        driver.execute_script(st_href)

                                        driver.implicitly_wait(2)

                                        district_row_select = Select(
                                            driver.find_element(
                                                By.XPATH,
                                                '//*[@id="example_length"]/label/select',
                                            )
                                        )

                                        district_row_select.select_by_value("100")

                                        driver.implicitly_wait(2)

                                        driver.execute_script(dist_href)
                                        
                                        driver.execute_script(block_href)

                                        driver.implicitly_wait(2)    
                                               
                            BackButton = driver.find_element(
                                By.XPATH,
                                '//*[@id="panelfilter"]/ul/li[2]/div/input[2]',
                            ).click()

                            break
                                
                        except NoSuchElementException:
                            print(
                                f"No Such Element raised at {state} {district} {block}"
                            )

                            driver.get(url)
                            driver.implicitly_wait(2)

                            driver.execute_script(st_href)

                            driver.implicitly_wait(2)

                            district_row_select = Select(
                                driver.find_element(
                                    By.XPATH,
                                    '//*[@id="example_length"]/label/select',
                                )
                            )

                            district_row_select.select_by_value("100")

                            driver.implicitly_wait(2)

                            driver.execute_script(dist_href)

                            driver.implicitly_wait(2)
                            
                with open(str(district_json_path),"w") as nested_out_file:
                    json.dump(
                        district_list, nested_out_file, ensure_ascii=False
                    )
                    
                            
            else:
                print(
                    district_json_path,
                    "exists and skipped. Moving to next district.",
                )
                continue
            BackButton = driver.find_element(
                By.XPATH,
                '//*[@id="panelfilter"]/ul/li[2]/div/input[2]',
            ).click()
            
        except (
            NoSuchElementException,
            TimeoutException,
            UnicodeEncodeError,
        ) as ex:

            while True:
                try:

                    print(f"{ex} raised at {state} {district} {block}")

                    driver.get(url)

                    driver.implicitly_wait(2)

                    driver.execute_script(st_href)

                    driver.implicitly_wait(2)

                    break

                except (NoSuchElementException, TimeoutException):
                    continue

        except WebDriverException as ex:

            while True:
                try:
                    print(f"{ex} raised at {state} {district} {block}")

                    driver.close()

                    # defining Chrome options
                    chrome_options = webdriver.ChromeOptions()
                    prefs = {
                        "download.default_directory": str(dir_path),
                        "profile.default_content_setting_values.automatic_downloads": 1,
                    }
                    chrome_options.add_experimental_option("prefs", prefs)
                    chrome_options.add_argument("start-maximized")

                    driver = webdriver.Chrome(
                        ChromeDriverManager().install(), options=chrome_options
                    )

                    driver.get(url)

                    driver.implicitly_wait(2)

                    driver.execute_script(st_href)

                    driver.implicitly_wait(2)

                    break

                except (NoSuchElementException, TimeoutException):
                    continue


driver.close()

print("Looping has ended. Scraper rests.")