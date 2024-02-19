import json
import re
from pathlib import Path
from selenium import webdriver
from selenium.common.exceptions import (NoSuchElementException, TimeoutException, WebDriverException,JavascriptException)
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
    
    def __init__(self):
        """
        Initialises the Scraper
        
        """    
        self.dir_path = Path.cwd()
        self.raw_path = Path.joinpath(self.dir_path, "data", "raw", "lgd_mapping_report_codes")
        if not self.raw_path.exists():
            Path.mkdir(self.raw_path, parents=True)
        self.interim_path = Path.joinpath(self.dir_path, "data", "interim")
       
         #defining Chrome options
        self.chrome_options = webdriver.ChromeOptions()
        self.chrome_options.add_argument("start-maximized")
        self.chrome_options.add_argument("--headless")
        self.chrome_options.add_argument("--ignore-certificate-errors")
        self.chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
        self.prefs = {"download.default_directory": str(self.dir_path), "profile.default_content_setting_values.automatic_downloads": 1,}
        self.chrome_options.add_experimental_option("prefs", self.prefs)
        self.driver = webdriver.Chrome(service=Service(executable_path=ChromeDriverManager().install()), options=self.chrome_options)

       
    def fetch_data(self, url):
        """
        Fetches data from the given url
        
        Args:
            url(str): url of website to be scraped
        
        """        
        self.driver.get(url)
       # wait = WebDriverWait(self.driver, 30)
        
    def row_select_100(self):
        """
        Sets the number of rows displayed on the webpage table to 100.
        """
        wait = WebDriverWait(self.driver, 10)
        dropdown = wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="example_length"]/label/select')))
        
        no_of_rows = Select(dropdown)
        no_of_rows.select_by_value("100")
    
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
    
    def element_attributes(self, xpath:str,name=None):
        """
        Get the values of the "text" and "href" attributes of all elements while passing the xpath of that granularity's table. 
        Args:
            xpath(str) : reprsents the xpath of that granularity's table
            
        Returns:
            tuple: Returns a tuple of two lists(names, hrefs)

        """
        element_table = self.driver.find_elements(By.XPATH, xpath)
        element_names = [x.get_attribute("text") for x in element_table]
        element_hrefs = [x.get_attribute("href") for x in element_table]
        
        if name == True:
            return element_names, element_hrefs
        else:
            return element_hrefs
        
    
    def back_button(self):
        """
        Clicks on the back button 
        
        """
        
        self.BackButton = self.driver.find_element(By.XPATH, '//*[@id="backButtonId"]/input', ).click() 
        
    def scrape(self):
        state_names, state_hrefs = self.element_attributes("//a[contains(@href, 'javascript:') and contains(@href, 'getDetail')]", name=True)
        for _,(state, st_href) in enumerate(zip(state_names, state_hrefs)):
            print(state)
            self.all_states_file_path = Path.joinpath(self.raw_path, "all_states.csv")
            self.state_folder_path = Path.joinpath(self.raw_path, state.lower().replace(" ", "_"))
            self.state_name_corrected = re.sub(r"[^A-Za-z0-9_]", "", state.lower().strip().replace(" ", "_"))
            self.district_file_path = Path.joinpath(self.state_folder_path, "district.csv")
            self.block_file_path = Path.joinpath(self.state_folder_path, "block.csv")
            self.gp_file_path = Path.joinpath(self.state_folder_path, "gp.csv")
            self.vill_file_path = Path.joinpath(self.state_folder_path,"village.csv")
            if not self.all_states_file_path.exists():
                print("Scraping main page table...")
                granularity = "main"
                self.scrape_table(granularity)
                
            if not self.state_folder_path.exists():
                Path.mkdir(self.state_folder_path, parents=True)
                
            # Constructing hrefs for district, block, gp, vill
            state_id = st_href.split("'")[1]
            dist_href = f"javascript:getLgdListDistrictMapped('{state_id}','{state}');"
            block_href = f"javascript:getLgdListBlockMapped('{state_id}','{state}');"
            gp_href = f"javascript:getLgdListGpMapped('{state_id}','{state}');"
            vill_href = f"javascript:getLgdListVillageMapped('{state_id}','{state}');"
           
            if not self.district_file_path.exists():
                print("Scraping district table..")
                self.driver.execute_script(dist_href)
                self.row_select_100()
                granularity = "district"
                self.scrape_table(granularity)
                print(f"Saved district file for {state}")
            
            if not self.block_file_path.exists():
                print("Scraping block table..")
                self.driver.execute_script(block_href)
                self.row_select_100()
                granularity = "block"
                self.scrape_table(granularity)
                print(f"Saved block file for {state}")
           
            if not self.gp_file_path.exists():
                print("Scraping gp table..")
                self.driver.execute_script(gp_href)
                self.row_select_100()
                granularity = "gp"
                self.scrape_table(granularity)
                print(f"Saved gp file for {state}")
                
            if not self.vill_file_path.exists():
                print("Scraping village table..")
                self.driver.execute_script(vill_href)
                self.row_select_100()
                granularity = "vill"
                self.scrape_table(granularity)
                print(f"Saved village file for {state}")

    def scrape_table(self, granularity):
        if granularity=="main":
            xpath = '/html/body/div[4]/form/div[3]/div/div/table/tbody/tr/td/table'
        else:
            xpath = '//*[@id="example"]'
        table_element = self.driver.find_element(By.XPATH, xpath)
        table_element_html = table_element.get_attribute("outerHTML")
        table = pd.read_html(table_element_html)[0]
        while Scraper.click_next_button(self):
            table_element_new = self.driver.find_element(By.XPATH, '//*[@id="example"]')
            table_element_html_new = table_element_new.get_attribute("outerHTML")
            table_new= pd.read_html(table_element_html_new)[0]
            table=table.append(table_new)
        if granularity=="main":
            table.columns = table.columns.map('_'.join).str.strip('_')
        table = table.rename(columns=lambda x: x.replace(' ', '_').lower())
        columns_to_rename = [('s.no._s.no.','sno'),('state_state','state_name'),('mis-nrlm_code_mis-nrlm_code', 'mis_nrlm_code'),('lgd_code_lgd_code', 'lgd_code'),('mis-nrlm_code','mis_nrlm_code'),('s.no.','sno')]
        
        for old, new in columns_to_rename:
            table.rename(columns={old:new},inplace=True)
        table = table[pd.to_numeric(table["sno"], errors='coerce').notnull()]
        table.drop('sno', inplace=True, axis=1)
        print(table.dtypes)
        if granularity == "main":
            if not self.raw_path.exists():
                Path.mkdir(self.raw_path, parents=True)
            table.to_csv(Path.joinpath(self.raw_path, "all_states.csv"), index=False)
        else:
            table = table.iloc[:, :3] 
        
        if granularity == "district":
            table = table.rename(columns={"mis_nrlm_code":"district_mis_id","lgd_code":"district_code"})
            for column_name in ['district_mis_id', 'district_code']:
                table[column_name] = table[column_name].astype(str)
                max_length = table[column_name].str.len().max()
                table[column_name] = table[column_name].str.zfill(max_length)

            print(table.dtypes)
            table.to_csv(Path.joinpath(self.state_folder_path, "district.csv"), index=False, float_format="%.0f")
        if granularity == "block":
            table = table.rename(columns={"mis_nrlm_code":"block_mis_id","lgd_code":"block_code"})
            table["block_mis_id"]= table["block_mis_id"].astype(str)
            table["block_code"]= table["block_code"].astype(str)
            print(table.dtypes)
            table.to_csv(Path.joinpath(self.state_folder_path, "block.csv"), index=False)
        if granularity == "gp":
            table = table.rename(columns={"mis_nrlm_code":"gp_mis_id","lgd_code":"gp_code"})
            table["gp_mis_id"]= table["gp_mis_id"].astype(str)
            table["gp_code"] = table["gp_code"].astype(str)
            print(table.dtypes)
            table.to_csv(Path.joinpath(self.state_folder_path, "gp.csv"), index=False)
        if granularity == "vill":
            table = table.rename(columns={"mis_nrlm_code":"vill_mis_id","lgd_code":"village_code"})
            table["vill_mis_id"]= table["vill_mis_id"].astype(str)
            table["village_code"]=table["village_code"].astype(str)
            table.to_csv(Path.joinpath(self.state_folder_path, "village.csv"), index=False)
    

    def merge_district_csv(self):
        
        state_folders = [x for x in self.raw_path.iterdir() if x.is_dir()]
        df = pd.DataFrame()

        for state_folder in state_folders:
            district_file_path = Path.joinpath(state_folder, "district.csv")
            if district_file_path.exists():
               
                state_df = pd.read_csv(district_file_path, dtype=str)
                #state_df["State"] = state_folder.name.replace("_", " ")
                
                df = df.append(state_df, ignore_index=True)
  
        df.to_csv(Path.joinpath(self.raw_path, "mapped_districts.csv"), index=False)
    def merge_granularity_csv(self, granularity):
        
        state_folders = [x for x in self.raw_path.iterdir() if x.is_dir()]
        df = pd.DataFrame()

        for state_folder in state_folders:
            granularity_file_path = Path.joinpath(state_folder, f"{granularity}.csv")
            if granularity_file_path.exists():
                state_df = pd.read_csv(granularity_file_path, dtype=str)
                df = df.append(state_df, ignore_index=True)

        self.mapped_file_path = Path.joinpath(self.raw_path, f"mapped_{granularity}s.csv")
        if not self.mapped_file_path.exists():
            df.to_csv(self.mapped_file_path, index=False)
            print(f"Appended all {granularity}s in a single file")
        else:
            print(f"{self.mapped_file_path} already exists. Skipping the merging of {granularity}s.")

    def duplicacy_check(self,granularity):
        if self.mapped_file_path.exists():
            df = pd.read_csv(self.mapped_file_path)
            if df['mis_nrlm_code'].duplicated().any():
                duplicates = df[df['mis_nrlm_code'].duplicated(keep=False)]
                print(f"Duplicate values in {granularity} 'mis_nrlm_code' column:")
                print(duplicates)
                duplicates_file_path = Path.joinpath(self.raw_path, f"duplicate_mis_{granularity}.csv")
                # Checking if names are same for same lgd_code
                mis_nrlm_code_counts = duplicates.groupby('mis_nrlm_code').size()
                for mis_nrlm_code, count in mis_nrlm_code_counts.items():
                    if count > 1:
                        names = duplicates[duplicates['mis_nrlm_code'] == mis_nrlm_code].iloc[:,0].unique()
                        if len(names) > 1:
                            print(f"Multiple names found for lgd_code '{mis_nrlm_code}' in {granularity} file.")
                            duplicates.loc[duplicates['lgd_code'] == mis_nrlm_code, 'name_lgd_code'] = duplicates.iloc[:, 0].astype(str) + '_' + duplicates['mis_nrlm_code'].astype(str)
        
                duplicates.to_csv(duplicates_file_path, index=False)
               
            else:
                print(f"All values in {granularity} 'lgd_code' column are unique.")
        else:
            print(f"{self.mapped_file_path} does not exist.")
      
if __name__==  '__main__':
    url = "https://nrlm.gov.in/LgdMappingReport.do?methodName=getLgdDetailAction&encd=n"
    scraper = Scraper()
    scraper.fetch_data(url)
    scraper.scrape()       
    # granularities = ["block", "district", "gp", "village"]
    # for granularity in granularities:
    #     scraper.merge_granularity_csv(granularity)
    #     scraper.duplicacy_check(granularity)

