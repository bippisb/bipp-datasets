# Import Libraries
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select
from selenium.webdriver.chrome.options import Options

# Chrome extension pop up window disable
options = webdriver.FirefoxOptions()
options.add_argument('--headless')

# initialize the webdriver
driver = webdriver.Firefox(options=options)

# navigate to the webpage containing the select element
driver.get("https://www.poshantracker.in/statistics")

time.sleep(5)

# Get value attribute of selectstate to getting list of it 
state = driver.find_element(By.NAME, "selectstate")
state_ele = Select(state)
state_list = [ e.get_attribute("value") for e in state_ele.options]

data = []
for e in state_ele.options[1:]:             # Looping for state
    sc =  e.get_attribute("value")
    state_ele.select_by_visible_text(e.text)
    district = driver.find_element(By.NAME, "selectdistrict")
    dist_ele = Select(district)
    dist_list = [ e.get_attribute("value") for e in dist_ele.options]
    
    for i in dist_ele.options[1:]:          # Looping for District
        dc =  i.get_attribute("value")
        dist_ele.select_by_visible_text(i.text)
        month = driver.find_element(By.ID, "sel1 monthSelection")
        month_ele = Select(month)
        month_list = [ e.get_attribute("value") for e in month_ele.options]
        
        for m in month_ele.options[0:]:     # Looping for Month
            month_n = m.get_attribute("value").split('_')[0]
            year_n = m.get_attribute("value").split('_')[1]

            # Dashboard Data from the json 
            """ In url ?v is the version number and it is used to get the latest data, so it should be updated when new data is added """
            url = f"https://cdn.poshantracker.in/pt_dashboard/{year_n}/{month_n}/{sc}/{dc}/{dc}_PT_Dashboard.json?v2023082415"
            response = requests.get(url)
            if response.status_code == 200:
                json_dash = response.json()
            else:
                print("Error: Failed to retrieve JSON data")
            # Extract the data from the dictionary
            dash_data = json_dash['registrationPTdata'][0]['districts'][0]['data']

            # Monthwise Active Beneficiaries data from the json
            url = f"https://cdn.poshantracker.in/pt_dashboard/{year_n}/{month_n}/{sc}/{dc}/{dc}_monthWiseActiveBeneficiaries.json?v2023082415"
            response = requests.get(url)
            if response.status_code == 200:
                json_month = response.json()
            else:
                print("Error: Failed to retrieve JSON data")
            # Extract the data from the dictionary
            aadhar_Health_Data = json_month['monthWiseActiveBeneficiaries'][0]['districts'][0]['aadharHealthData']
            aadhar_Health = [{'title': item['title']+' '+item['title1'],'count': item['count']} for item in aadhar_Health_Data]
            # Extract the data from the dictionary
            active_ben_Data = json_month['monthWiseActiveBeneficiaries'][0]['districts'][0]['activeBeneficiaries']
            active_ben = [{'title': item['title']+' '+item['title1'],'count': item['count']} for item in active_ben_Data]

            # Keyservices data from the json
            url = f"https://cdn.poshantracker.in/pt_dashboard/{year_n}/{month_n}/{sc}/{dc}/{dc}_keyServices_v2.json?v2023082415"

            response = requests.get(url)
            if response.status_code == 200:
                json_keyservice = response.json()
            else:
                print("Error: Failed to retrieve JSON data")
            # Extract the data from the dictionary
            key_data = json_keyservice['keyServices'][0]['districts'][0]['data']
            key_services = [{'title': item['title']+' '+item['title1'],'count': item['count']} for item in key_data]

            district_dict = dash_data + aadhar_Health + active_ben + key_services
            district_dict
            # Adding the item year_month, state name, district name in each dict
            for item in district_dict:      
                item['date'] = m.text
                item['state'] = e.text
                item['district'] = i.text
            # append all dicts into data list
            data.extend(district_dict) 
            print(f'Data Extracted for {e.text},{i.text},{m.text} ')

# close the webdriver
driver.quit()

# create the dataframe from the data list 
data_df = pd.DataFrame(data)
# rearrange the columns
data_df = data_df[['date','state','district','title','count']]  
# pivot the table on title elements  
pivoted_df = data_df.pivot_table(index=['date','state', 'district'], columns='title', values='count')  
pivoted_df.reset_index(inplace=True)
# Rename the Columns 
pivoted_df = pivoted_df.rename(columns={
       'AWC Infrastructure Drinking Water Source': 'awc_infra_dws',
       'AWC Infrastructure Fuctional Toilets': 'awc_infra_fun_toilets',
       'AWC Infrastructure Own Building': 'awc_infra_own_buil', 
       'Aadhaar verified Beneficiaries':'aadhar_verf_benf',
       'Adolescent Girls ':'adolescent_girls', 
       'Anganwadi Centers Open For Atleast 1 Day':'awc_open_1day',
       'Anganwadi Centers Open For Atleast 21 Days':'awc_open_21day', 
       'Anganwadi Workers':'aww',
       'Angwanwadi Centers':'awc', 
       'Children (0-6 Months)':'children_0_6_months', 
       'Children (3 - 6 Years)':'children_3_6_years',
       'Children (6 Months - 3 Years)':'children_6months_3years',
       'Children Attended AWC For Atleast 21 days':'children_awc_21days',
       'Children(3 - 6 Years) Non-school Going': 'children_3_6_years_nonschool',
       'Children(3 - 6 Years) School Going':'children_3_6_years_school', 
       'Eligible Beneficiaries':'elgb_benef', 
       'Health ID Created':'health_id_created',
       'Hot Cooked Meal (HCM) Given For 1-6 Days':'hcm_1_6days',
       'Hot Cooked Meal (HCM) Given For 15-20 Days':'hcm_15_20days',
       'Hot Cooked Meal (HCM) Given For 7-14 Days':'hcm_7_14days',
       'Hot Cooked Meal (HCM) Given For Atleast 1 Day':'hcm_atleast_1day',
       'Hot Cooked Meal (HCM) Given For Atleast 15 Days':'hcm_atleast_15days',
       'Hot Cooked Meal (HCM) Given For Atleast 21 Days':'hcm_atleast_21days', 
       'Lactating Mothers ':'lact_mothers',
       'Pregnant Women ':'pregnant_women', 
       'Project':'project', 
       'Sector':'sector', 
       'Take Home Ration Given For Atleast 1 Day':'thm_atleast_1day',
       'Take Home Ration Given For Atleast 21 Days':'thm_atleast_21days'}) 
# drop the unwanted columns             
pivoted_df = pivoted_df.drop(['States/UTs', 'Districts'], axis=1)    
# Remove NaN values
pivoted_df = pivoted_df.fillna(0) 
# Convert year into datetime                              
pivoted_df['date']= pd.to_datetime(pivoted_df['date'], format='%b %Y')                     
# save the file as csv
pivoted_df.to_csv('../data/raw/poshan_tracker.csv',index=False)                                     