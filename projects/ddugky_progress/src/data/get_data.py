import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options
from bs4 import BeautifulSoup
from selenium.common.exceptions import StaleElementReferenceException

# Define the URL
url = "https://dashboard.rural.nic.in/dashboardnew/ddugky.aspx"

# Configure Firefox options (headless mode)
firefox_options = Options()
firefox_options.add_argument("--headless")

# Create a Firefox WebDriver instance
driver = webdriver.Firefox(options=firefox_options)

# Open the URL in the browser
driver.get(url)

# Find the dropdown for financial year
fin_year_dropdown = Select(driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_ddlfin_year"))

# Extract the available options for financial year
fin_year_options = [option.text.strip() for option in fin_year_dropdown.options]
print(fin_year_options)

# Initialize an empty list to store the scraped data
data = []

# Loop through each financial year
for year in fin_year_options:
    while True:
        try:
            # Select the financial year
            fin_year_dropdown = Select(driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_ddlfin_year"))  
            fin_year_dropdown.select_by_visible_text(year)

            # Find the dropdown for state
            state_dropdown = Select(driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_ddlstate"))

            # Extract the available options for state
            state_options = [option.text.strip() for option in state_dropdown.options]

            # Loop through each state within the current financial year
            for state_name in state_options:
                while True:
                    try:
                        # Select the state
                        state_dropdown = Select(driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_ddlstate")) 
                        state_dropdown.select_by_visible_text(state_name)

                        # Wait for the state dropdown to stabilize
                        WebDriverWait(driver, 10).until(EC.text_to_be_present_in_element((By.ID, "ctl00_ContentPlaceHolder1_ddlstate"), state_name))

                        # Parse the updated page with the selected options
                        soup = BeautifulSoup(driver.page_source, "html.parser")

                        # Find the data elements
                        candidates_trained = soup.find("span", {"id": "ctl00_ContentPlaceHolder1_lbltrained"}).text.strip()
                        candidates_assessed = soup.find("span", {"id": "ctl00_ContentPlaceHolder1_lblassessed"}).text.strip()
                        candidates_placed = soup.find("span", {"id": "ctl00_ContentPlaceHolder1_lblplaced"}).text.strip()

                        # Append the data to the list
                        data.append({
                            "year": year,
                            "state_name": state_name,
                            "candidates_trained": candidates_trained,
                            "candidates_assessed": candidates_assessed,
                            "candidates_placed": candidates_placed,
                        })
                        print(f"Successful state selection for {year}, {state_name}, candidates_trained: {candidates_trained}, candidates_assessed: {candidates_assessed}, candidates_placed: {candidates_placed}")
                        break  # Exit the retry loop if successful
                        
                    except StaleElementReferenceException:
                        print(f"StaleElementReferenceException occurred for {year}, {state_name}. Retrying state selection...")
                        continue
            break  # Exit the retry loop if successful
        except StaleElementReferenceException:
            print(f"StaleElementReferenceException occurred for {year}. Retrying financial year selection...")
            continue

# Close the WebDriver
driver.quit()

# Create a DataFrame from the collected data
df = pd.DataFrame(data)
df.drop_duplicates(inplace=True)
df.drop(df[df['state_name'] == 'ALL STATE'].index, inplace=True)
df["state_name"] = df["state_name"].apply(lambda x: x.title())

# Save the DataFrame to a CSV file
df.to_csv("../data/interim/ddugky_data.csv", index=False)

print("Data has been scraped and saved to ddugky_data.csv.")
