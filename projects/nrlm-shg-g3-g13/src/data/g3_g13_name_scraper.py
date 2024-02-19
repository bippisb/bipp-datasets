import json
import re
from pathlib import Path
from selenium import webdriver
from selenium.common.exceptions import (NoSuchElementException, TimeoutException, WebDriverException, JavascriptException)
from urllib.error import HTTPError
import urllib.error
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import time
import pandas as pd

class Scraper:
    
    def __init__(self,url,driver=None):
        """
        Initialises the Scraper
        
        Args:
            time_stamp(str): The time stamp of when the Scraper is initialized and is used to create file directory paths
            url(str): The url of the website to be scraped
        
        """    
        self.dir_path = Path.cwd()
        self.raw_path = Path.joinpath(self.dir_path, "data", "raw", "jsons_with_mis_id")
        self.interim_path = Path.joinpath(self.dir_path, "data", "interim")
        self.url= url

         #defining Chrome options
        self.chrome_options = webdriver.ChromeOptions()
        self.chrome_options.add_argument("start-maximized")
        self.chrome_options.add_argument("--headless")
        self.chrome_options.add_argument("--ignore-certificate-errors")
        self.chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
        self.prefs = {"download.default_directory": str(self.dir_path), "profile.default_content_setting_values.automatic_downloads": 1,}
        self.chrome_options.add_experimental_option("prefs", self.prefs)
     
        if driver:
            self.driver = driver
        else:
   
            self.driver = webdriver.Chrome(service=Service(executable_path=ChromeDriverManager().install()), options=self.chrome_options)
        
        
    def fetch_data(self, url):
        """
        Fetches data from the given url
        
        Args:
            url(str): url of website to be scraped
        
        """        
        self.driver.get(self.url)
        
    def row_select_100(self):
        """
        Sets the number of rows displayed on the webpage table to 100.
        """
        wait = WebDriverWait(self.driver, 10)
        dropdown = wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="example_length"]/label/select')))
        
        no_of_rows = Select(dropdown)
        no_of_rows.select_by_value("100")
        self.driver.implicitly_wait(2)
    
    def click_next_button(self)-> bool:
        """
        Checks for the presence of a Next button and clicks it if it exists.

        Returns:
            bool: Returns True if Next button is clicked, False otherwise.
        """
        try:
            next_button = self.driver.find_element(By.XPATH, '//*[@id="example_next"]')
            next_page_class = next_button.get_attribute("class")

            if next_page_class != "paginate_button next disabled":
                print("Next button exists. Scraping from next page...")
                next_button.click()
                return True
            else:
                return False
        except:
            return False
    
    def element_attributes(self, xpath:str, mis=None):
        """
        Get the values of the "text" and "href" attributes of all elements while passing the xpath of that granularity's table. It also checks for next page, if it exists it clicks onto it and concatenates "href" and "text" attribute from each page.
        
        Args:
            xpath(str) : reprsents the xpath of that granularity's table
            
        Returns:
            tuple: Returns a tuple of three lists(names,href, mis_id of a granularity) if mis is True
            tuple: Returns a tuple of two lists(names, hrefs)

        """
        element_table = self.driver.find_elements(By.XPATH, xpath)
        element_names = [x.get_attribute("text") for x in element_table]
        element_hrefs = [x.get_attribute("href") for x in element_table]
        split_hrefs = [href.split("'") for href in element_hrefs]
        # #  second element in each list (index 1)
        mis_id = [href[1] for href in split_hrefs]
        mis_id = [item.strip("' '") for item in mis_id]
        
        while self.click_next_button():
            self.driver.implicitly_wait(2)
            element_table = self.driver.find_elements(By.XPATH, "//*[@id='example']//a")
            element_names_next = [x.get_attribute("text") for x in element_table]
            element_names.extend(element_names_next)
            element_hrefs_next = [x.get_attribute("href") for x in element_table]
            element_hrefs.extend(element_hrefs_next)
            split_hrefs_next = [href.split("'") for href in element_hrefs_next]
            mis_id_next = [href[1] for href in split_hrefs_next]
            mis_id_next = [item.strip("' '") for item in mis_id_next]
            mis_id.extend(mis_id_next)
            self.driver.implicitly_wait(2)
        
        if mis==True:    
            return element_names, element_hrefs, mis_id
        else:
            return element_names, element_hrefs
    
    def back_button(self):
        """
        Clicks on the back button 
        
        """
        
        self.BackButton = self.driver.find_element(By.XPATH, '//*[@id="panelfilter"]/ul/li[2]/div/input[2]', ).click()

    def go_back_row_select(self,no_of_times):
    
        """
        This function goes back a certain number of times and selects 100 rows of the table.
        """

        for _ in range(no_of_times):
            self.back_button()
            self.row_select_100()        

    def navigate_page(self,href,row_select = True):

        """
        Navigates to a new page based on the provided href and optionally selects 100 rows of the table.

        Args:
            href (str): The href to navigate to.
            row_select (bool): Whether to select 100 rows of the table. Default is True.
        """

        if row_select:
            self.row_select_100()
        self.driver.execute_script(href)
        self.driver.implicitly_wait(2)
        self.row_select_100()
        
    def scrape_state(self):
        """
        Scrapes the state name. It first captures the state name and href using "element_attributes", moves on to the district page and selects 100 rows of the table and calls the "scrape_district" method
        
        """
        
        state_names, state_hrefs, state_mis_nrlm_id = self.element_attributes('//*[@id="mainex"]//a',mis=True)
        for state, st_href, state_id in zip(state_names, state_hrefs, state_mis_nrlm_id):
            self.navigate_page(st_href, row_select=False)
            self.scrape_district(state, state_id, st_href)
                
          
    def scrape_district(self, state, state_id, st_href):
        """
        Scrapes the district page for the given state by capturing its attributes using "element_attributes" then goes to the block page by calling "scrape_block" method for that district
        
        Args:
            state(str): The name of the state being scraped.
            st_href(str): The href of the state being scraped.
            
        The scraped data is stored ina json file.
        """
        district_names, district_hrefs, dist_mis_nrlm_id = self.element_attributes("//*[@id='example']//a", mis=True)
        for district, district_href, dist_id in zip(district_names, district_hrefs, dist_mis_nrlm_id):
            try:
                district_list =[]
                state_path = Path.joinpath(self.raw_path, state.lower().replace(" ", "_"))
                json_name = ".".join([district.lower().replace(" ", "_"), "json"])
                district_json_path = Path.joinpath(self.raw_path, state.lower().replace(" ", "_"), json_name)

                if not state_path.exists():
                    Path.mkdir(state_path, parents=True)

                if not district_json_path.exists():
                    print(
                        district_json_path,
                        "doesn't exist and proceeding for scraping",
                    )
                    self.navigate_page(district_href)
                    self.scrape_block(state, state_id, district, dist_id, st_href, district_href, district_list)
                    with open(str(district_json_path), "w") as nested_out_file:
                        json.dump(
                            district_list, nested_out_file, ensure_ascii=False
                        )

                else:
                    print(
                        district_json_path,
                        "exists and skipped. Moving to next district.",
                    )
                    continue
                self.back_button()
                    
            except (NoSuchElementException, TimeoutException, UnicodeEncodeError) as ex:
                print(f"{ex} raised at {state} {district}")
                self.dist_exceptions(st_href)
                    
            except WebDriverException as ex:
                print(f"{ex} raised at {state} {district}")
                self.webdriver_exception(st_href)
                
    def scrape_block(self, state, state_id, district, dist_id, st_href, district_href, district_list):
        """Scrapes the block page for the given district by capturing its attributes using "element_attributes" then goes to the gp page by "scrape_gp" for that block

        Args:
            state (str): The name of the state being scraped.
            district (str): The name of the district being scraped.
            st_href (str): The href of the state being scraped.
            district_href (str): The href of the district being scraped.
            district_list (list): A list to store the scrapped data for the district.
        """
        block_names, block_hrefs, block_mis_nrlm_id = self.element_attributes("//*[@id='example']//a", mis=True)        
        for block, block_href, block_id in zip(block_names, block_hrefs, block_mis_nrlm_id):          
            while True:
                try:
                    print(
                        f"Attempting scrape of {state} {district} {block}"
                    )
                    self.navigate_page(block_href)
                    self.scrape_gp(state, state_id, district, dist_id, block, block_id, st_href, district_href, block_href,district_list)          
                    self.back_button()
                    break
                except NoSuchElementException:
                    print(
                    f"No Such Element raised at {state} {district} {block}"
                    )
                    self.block_exceptions(st_href, district_href)
                            
    def scrape_gp(self, state, state_id, district, dist_id, block, block_id, st_href, district_href, block_href,district_list):
        """
        Scrapes all the GPs from a particular block and calls the `scrape_vill` method for each GP to scrape its villages.

        Args:
            state (str): The name of the state to which the GP belongs.
            district (str): The name of the district to which the GP belongs.
            block (str): The name of the block for which the GPs are scraped.
            st_href (str): The href of the stateto which the GP belong.s
            district_href (str): The href of the district to which the GP belong.
            block_href (str): The href of block for which the Gps are being scraped
            district_list (str): The list that holds the scraped data and gp names will be appended in it.
        """
        gp_names, gp_hrefs, gp_mis_nrlm_id  = self.element_attributes("//*[@id='example']//a",mis=True)        
        for gp, gp_href, gp_id  in zip(gp_names, gp_hrefs, gp_mis_nrlm_id):
            while True:
                try:
                    print(
                        f"Attempting scrape of {state} {district} {block} {gp}"
                    )
                    self.navigate_page(gp_href)
                    self.scrape_vill(state, state_id, district, dist_id, block, block_id, gp, gp_id, st_href, district_href, block_href,gp_href, district_list)
                    self.back_button()
                    break
                except (NoSuchElementException, TimeoutException, UnicodeEncodeError):
                    print(
                    f"No Such Element raised at {state} {district} {block} {gp}"
                    )
                    self.gp_exceptions(st_href, district_href, block_href)
                except(JavascriptException) as ex:
                    print(f"{ex} Javascript Exception raised at {state} {district} {block} {gp}")
                    self.gp_exceptions(st_href, district_href, block_href)
                    break
                except(Exception) as ex:
                    print(f"{ex} Exception raised at {state} {district} {block} {gp} ")
                    self.gp_exceptions(st_href, district_href, block_href)
                    break 
               
                    
    def scrape_vill(self, state, state_id, district, dist_id, block, block_id, gp, gp_id, st_href, district_href, block_href,gp_href, district_list):
        """
        Extracts the names and links of villages within a gram panchayat and adds them to a district list.

        Args:
            state (str): The name of of the state to which the village belongs.
            district (str): The name of the district to which the village belongs.
            block (str): The name of the block to which the village belongs.
            gp (str): The name of the gp to which the village belongs.
            district_list (list): The list to which the village data will be appended.
        """
        village_names, village_hrefs, vill_mis_nrlm_id  = self.element_attributes("//*[@id='example']//a", mis =True)        
        for vill, vill_href, vill_id  in zip(village_names, village_hrefs, vill_mis_nrlm_id):
            while True:
                try:
                    print(f"Attempting scrape of {state} {district} {block} {gp} {vill}")
                    self.navigate_page(vill_href)
                    self.scrape_shg(state, state_id, district, dist_id, block, block_id, gp, gp_id, vill, vill_id, district_list)
                    self.back_button()
                    break
                except (NoSuchElementException, TimeoutException, UnicodeEncodeError):
                    print(
                    f"No Such Element raised at {state} {district} {block} {gp} {vill}"
                    )
                    self.vill_exceptions(st_href, district_href, block_href,gp_href)
                except(JavascriptException) as ex:
                    print(f"{ex} Javascript Exception raised at {state} {district} {block} {gp} {vill}")
                    self.vill_exceptions(st_href, district_href, block_href,gp_href)
                    break   
                except(Exception) as ex:
                    print(f"{ex} Exception raised at {state} {district} {block} {gp} {vill}")
                    self.vill_exceptions(st_href, district_href, block_href,gp_href)
                    break    
           
    
    def scrape_shg(self, state, state_id, district, dist_id, block, block_id, gp, gp_id, vill, vill_id, district_list):
        group_table = self.driver.find_elements(By.XPATH, "//*[@id='example']//a")
        group_names = [x.get_attribute("text") for x in group_table]
        group_hrefs = [ x.get_attribute("href") for x in group_table]
        gr_mis = [href.split("'")[1] for href in group_hrefs]
        gr_id = [href.split(",")[1] for href in group_hrefs]
        gr_id = [item.strip("' '") for item in gr_id]        
        while True:
            NextButton = self.driver.find_element( By.XPATH, '//*[@id="example_next"]')
            next_page_class = NextButton.get_attribute("class")
            if next_page_class != "paginate_button next disabled":
                NextButton.click()
                print(
                    f"NEXT Button exists for {state} {district} {block} {gp} {vill}. Scraping the next page."
                )
                self.driver.implicitly_wait(2)                
                group_table = self.driver.find_elements(By.XPATH, "//*[@id='example']//a")                
                group_names_next = [x.get_attribute("text")for x in group_table]
                group_names.extend(group_names_next)
                group_hrefs_next = [x.get_attribute("href")for x in group_table]
                group_hrefs.extend(group_hrefs_next)
                gr_mis_next = [href.split("'")[1] for href in group_hrefs_next]
                gr_id_next = [href.split(",")[1] for href in group_hrefs_next]
                gr_id_next = [item.strip("' '") for item in gr_id_next]
                gr_id.extend(gr_id_next)
                gr_mis.extend(gr_mis_next)
                self.driver.implicitly_wait(2)

            else:
                break            
        for i,g in enumerate(group_names):
            #name_split= [ i for i in g]            
            try:         
                if ((str.isascii(district)) and (str.isascii(block)) and (str.isascii(gp)) and (str.isascii(vill)) and (str.isascii(g))):
                    vill_dict ={}                    
                    vill_dict = {
                        "state_name": state,
                        "state_mis_id": state_id,
                        "district_name": district,
                        "district_mis_id": dist_id,
                        "block_name": block,
                        "block_mis_id": block_id,
                        "gp_name": gp,
                        "gp_mis_id": gp_id,
                        "vill_name": vill,
                        "vill_mis_id": vill_id,
                        "shg_name": g,
                        "shg_id": gr_id[i],
                        "shg_mis_id": gr_mis[i]
                    }
                    district_list.append(vill_dict)       
                else:
                    pass
            except UnicodeEncodeError:
                continue
    
                    
    def vill_exceptions(self, st_href, district_href, block_href, gp_href):
        self.gp_exceptions(st_href, district_href, block_href)
        self.navigate_page(gp_href)
        
    
    def gp_exceptions(self, st_href, district_href, block_href):
        self.block_exceptions(st_href,district_href)
        self.navigate_page(block_href)
       
                                   
    def block_exceptions(self,st_href, district_href):   
        self.fetch_data(self.url)
        self.navigate_page(st_href, row_select = False)
        self.navigate_page(district_href, row_select = False)
       
    def dist_exceptions(self, st_href):
        while True:
            try:
                
                self.fetch_data(self.url)
                self.driver.execute_script(st_href)
                self.driver.implicitly_wait(2)
                break
            except (NoSuchElementException, TimeoutException):
                continue 
    
    def webdriver_exception(self, st_href):
        while True:
            try:
                
                self.driver.close()    
                new = Scraper(self.url)
                new.fetch_data(self.url)  
                self.navigate_page(st_href, row_select = False)
                self.driver = new.driver
                break
            except (NoSuchElementException, TimeoutException):
                continue   

##########################################################################
class JsonFlat(Scraper):
    """
    Child class of Scraper used for flattening JSON data.
    Provides a method to concatenate all JSON files, flatten it and save it in a JSON file.
    """
    
    def __init__(self, url,driver):
        """
        Initialises the class
        
        Args:
            time_stamp(str): The time stamp of when the Scraper is initialized and is used to create file directory paths
            url(str): The url of the website to be scraped
        """
        
        super().__init__( url,driver)
        self.all_names_path = Path.joinpath(self.interim_path, "all_names.json")
        self.driver = driver
        
    def concat_and_flatten(self):
        """
        Concatenates all JSON files, flattens it and saves it in a JSON file.
        """
        
        all_names = []
        
        files = list(self.raw_path.glob("*/*.json"))
        
        for file in files:
            with open(file, "r") as outfile:
                print(file)
                dist_level_names = json.load(outfile)

            all_names.extend(dist_level_names)

        with open(str(self.all_names_path), "w") as outfile:
            json.dump(all_names, outfile, ensure_ascii=False)

        print(len(all_names))

if __name__ == "__main__":
    url = "https://nrlm.gov.in/shgReport.do?methodName=showMojorityStateWise"
    name = Scraper(url)
    name.fetch_data(url)
    name.scrape_state()
   