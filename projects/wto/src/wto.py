from pathlib import Path
from playwright.sync_api import sync_playwright
import pandas as pd

def retry_until_found(func, *args, max_retries=3):
    for attempt in range(max_retries):
        try:
            return func(*args)
        except Exception as e:
            print(f"Error: {str(e)} - Retrying... (Attempt {attempt + 1}/{max_retries})")
    raise Exception(f"Failed after {max_retries} attempts.")

def process_checkbox(page, checkbox_id, prev_text, max_attempts=2000):
    records = []  # List to store records

    attempt = 0
    while True:
        current_checkbox_selector = f'#ctl00_ContentView_pn{checkbox_id}CheckBox'
        checkbox_element = retry_until_found(page.query_selector, current_checkbox_selector)

        if not checkbox_element or attempt >= max_attempts:
            break

        page.wait_for_timeout(timeout=1000)
        checkbox_text_element = retry_until_found(page.query_selector, f'#ctl00_ContentView_pt{checkbox_id}')
        checkbox_text = checkbox_text_element.inner_text()

        try:
            page.wait_for_timeout(timeout=1000)  # Wait before checkbox check

            if checkbox_text[:4] == prev_text[:4]:
                prev_text = checkbox_text[:4]
                records.append(["", "", checkbox_text])
                checkbox_element.check()  # Click the checkbox
                page.wait_for_timeout(timeout=1000)  # Wait after checkbox click
                checkbox_id += 1

            elif checkbox_text[:2] == prev_text[:2]:
                prev_text = checkbox_text[:4]
                records.append(["", checkbox_text, ""])
                checkbox_text_element.click()  # Click the checkbox
                page.wait_for_timeout(timeout=1000)  # Wait after checkbox click
                checkbox_id += 1

            else:
                prev_text = checkbox_text[:4]
                records.append([checkbox_text, "", ""])
                checkbox_text_element.click()  # Click the checkbox
                page.wait_for_timeout(timeout=1000)  # Wait after checkbox click
                checkbox_id += 1

        except Exception as e:
            print(f"Error processing checkbox {checkbox_id}: {str(e)}")

        attempt += 1

    return records, checkbox_id, prev_text

def process_subcategory(page, subcategory_id, prev_text, max_attempts=2000):
    records = []  # List to store records

    attempt = 0
    while True:
        current_subcategory_selector = f'#ctl00_ContentView_pt{subcategory_id}'
        subcategory_element = retry_until_found(page.query_selector, current_subcategory_selector)

        if not subcategory_element or attempt >= max_attempts:
            break

        try:
            page.wait_for_timeout(timeout=1000)  # Wait before subcategory click

            subcategory_element.click()
            subcategory_element.scroll_into_view_if_needed() 
            page.wait_for_timeout(timeout=2000)  # Increase timeout

            subcategory_text = subcategory_element.inner_text()
            records.append(["", subcategory_text, ""])

            checkbox_id = subcategory_id + 1
            checkbox_records, new_subcategory_id, new_prev_text = process_checkbox(page, checkbox_id, prev_text)
            records.extend(checkbox_records)

        except Exception as e:
            print(f"Error processing subcategory {subcategory_id}: {str(e)}")

        attempt += 1

    return records, new_subcategory_id, new_prev_text

def process_category(page, category_id, prev_text, max_attempts=2000):
    records = []  # List to store records

    attempt = 0
    while True:
        current_category_selector = f'#ctl00_ContentView_pt{category_id}'
        category_element = retry_until_found(page.query_selector, current_category_selector)

        if not category_element or attempt >= max_attempts:
            break

        try:
            page.wait_for_timeout(timeout=1000)  # Wait before category click

            category_element.click()
            category_element.scroll_into_view_if_needed()  
            page.wait_for_timeout(timeout=2000) 
            category_text = category_element.inner_text()
            records.append([category_text, "", ""])

            subcategory_id = category_id + 1
            subcategory_records, new_category_id, new_prev_text = process_subcategory(page, subcategory_id, prev_text)
            records.extend(subcategory_records)

        except Exception as e:
            print(f"Error processing category {category_id}: {str(e)}")

        attempt += 1

    return records, new_category_id, new_prev_text

def main():
    script_path = Path(__file__).resolve()
    data_folder = script_path.parents[1] / "data" / "raw"  # Navigate to the "Data" folder

    excel_path = data_folder / "output.xlsx"

    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=False)
            context = browser.new_context()
            page = context.new_page()
            url = "http://tariffdata.wto.org/ReportersAndProducts.aspx"
            page.goto(url)

            checkbox_selector = '#ctl00_ContentView_rn304CheckBox'
            page.check(checkbox_selector)
            page.wait_for_timeout(timeout=1000)
            records = []
            category_id = 0
            prev_text = "0101"

            category_records, _, _ = process_category(page, category_id, prev_text)
            records.extend(category_records)

            # Create DataFrame from records
            df = pd.DataFrame(records, columns=["category", "subcategory", "HS-Code"])

            # Save the DataFrame to Excel workbook
            df.to_excel(excel_path, index=False, sheet_name="Sheet1")

            input("Press Enter to close the browser...")

        except Exception as e:
            print(f"Error in main: {str(e)}")

if __name__ == "__main__":
    main()
