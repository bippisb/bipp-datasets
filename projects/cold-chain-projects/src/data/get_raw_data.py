from playwright.sync_api import sync_playwright
from pathlib import Path
import time
import os

def print_green(text): print("\033[92m {}\033[00m" .format(text))
def print_red(text): print("\033[91m {}\033[00m" .format(text))
def print_yellow(text): print("\033[93m {}\033[00m" .format(text))

download_path = Path(__file__).parents[2].joinpath("data/raw")

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True, slow_mo=500)
    # Explicitly configure download behavior for the context
    context = browser.new_context( accept_downloads=True,)
    page = context.new_page()

    try:
        # Navigate to the page
        page.goto("https://nhb.gov.in/OnlineClient/rptMISCrops_midh.aspx", timeout=120000)
    except Exception as e:
        print_red(f"Error navigating to the page: {str(e)}")

    wait_for_agency = page.wait_for_selector("#rdpCategory", timeout=120000)
    agency = page.locator("#rdpCategory")
    for a in agency.locator("option").all()[1:]:
        agency_text = a.text_content().replace("/", "_")
        agency_value = a.get_attribute("value")
        print("Agency: ", agency_text)

        # Select the agency
        agency.select_option(value=agency_value)
        wait_for_state = page.wait_for_selector("#drpState", timeout=120000)
        state = page.locator("#drpState")

        for s in state.locator("option").all()[1:]:
            state_text = s.text_content()
            state_value = s.get_attribute("value")
            print("State: ", state_text)
            # Select the state
            state.select_option(value=state_value)
            # Click on search
            page.locator("#btnSearch").click()
            # Wait for table
            wait_for_table = page.wait_for_selector("#gvdetails", timeout=120000, state="visible")
            time.sleep(10)         
            
            retry = 0
            while retry < 3:
                try:
                    # If #gvdetails_ctl02_lnkTotalApp is present
                    if page.locator("#gvdetails_ctl02_lnkTotalApp").is_visible():
                        # if #gvdetails_ctl02_lnkTotalApp text is not 0
                        if page.locator("#gvdetails_ctl02_lnkTotalApp").text_content() != "0":
                            print(f"Number of Projects: {page.locator('#gvdetails_ctl02_lnkTotalApp').text_content()}")

                            with page.expect_popup(timeout=120000) as popup_info:
                                page.locator("#gvdetails_ctl02_lnkStateName").click()
                                print(f"Popup found for {agency_text} {state_text}")
                                popup = popup_info.value

                                # get the data from #gridReport_Subsidy table and save it to a csv file
                                table = popup.locator("#gridReport_Subsidy")
                                table.wait_for(timeout=120000)

                                # Fetching headers
                                headers = ['Sl. No.','Project Code','State','Year of Subsidy Sanctioned','District','Name of Beneficiary','Project Address','Total Project Cost(In Lakhs)','Total Amount Sanctioned (In Lakhs) ','Current Status of Project',
                                           'Component Category','Component','No. of Unit(s)','Capacity','Unit of Capacity']

                                # Extracting row data
                                rows_data = []
                                rows_data.insert(0, headers)
                                for row in table.locator("tbody tr").all():
                                    row_data = [td.text_content().strip().replace(',', '-') for td in row.locator("td").all()]
                                    rows_data.append(row_data)

                                # Create a CSV file
                                csv_file_path = download_path / f"{agency_text}-{state_text}.csv"
                                # Write the table data to the CSV file
                                with open(csv_file_path, "w", newline='') as csv_file:
                                    # Write rows
                                    for row_data in rows_data:
                                        csv_file.write(','.join(row_data) + "\n")

                                print_green(f"CSV file saved to {csv_file_path}")
                            popup.locator("#btnClose").click()
                            time.sleep(3)
                            break
                        else:
                            print("Number of Projects: 0")
                            print_yellow(f"No projects found for {agency_text} {state_text}")
                            break
                    else:
                        print_yellow("Record not found")
                        break
                except Exception as e:
                    print_red(f"Error: {str(e)} for {agency_text} {state_text}", )
                    retry += 1
                    popup.close()
                    print_yellow("Retrying...")
                
    browser.close() 