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

def process_checkbox(page, checkbox_id, subcategory_base_selector, subcategory_id, category_id, prev_text, max_attempts=2000):
    records = []  # List to store records

    for attempt in range(max_attempts):
        current_checkbox_selector = f'#ctl00_ContentView_pn{checkbox_id}CheckBox'
        checkbox_element = retry_until_found(page.query_selector, current_checkbox_selector)

        if not checkbox_element:
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
                subcategory_id += 1

            elif checkbox_text[:2] == prev_text[:2]:
                prev_text = checkbox_text[:4]
                records.append(["", checkbox_text, ""])
                checkbox_text_element.click()  # Click the checkbox
                page.wait_for_timeout(timeout=1000)  # Wait after checkbox click
                checkbox_id += 1
                subcategory_id += 1

            else:
                prev_text = checkbox_text[:4]
                records.append([checkbox_text, "", ""])
                checkbox_text_element.click()  # Click the checkbox
                page.wait_for_timeout(timeout=1000)  # Wait after checkbox click
                checkbox_id += 1
                subcategory_id += 1

        except Exception as e:
            print(f"Error processing checkbox {checkbox_id}: {str(e)}")

    return records

def process_subcategory(page, subcategory_base_selector, subcategory_id, category_id, prev_text):
    records = []  # List to store records

    current_subcategory_selector = f'#ctl00_ContentView_pt{subcategory_id}'
    subcategory_element = retry_until_found(page.query_selector, current_subcategory_selector)

    if not subcategory_element:
        return records

    try:
        page.wait_for_timeout(timeout=1000)  # Wait before subcategory click

        subcategory_element.click()
        subcategory_element.scroll_into_view_if_needed()  # Scroll into view
        page.wait_for_timeout(timeout=2000)  # Increase timeout

        subcategory_text = subcategory_element.inner_text()
        records.append(["", subcategory_text, ""])

        checkbox_id = subcategory_id + 1
        checkbox_records = process_checkbox(page, checkbox_id, subcategory_base_selector, subcategory_id, category_id, prev_text)
        records.extend(checkbox_records)

    except Exception as e:
        print(f"Error processing subcategory {subcategory_id}: {str(e)}")

    return records

def process_category(page, category_id, subcategory_base_selector, checkbox_base_selector, prev_text):
    records = []  # List to store records

    current_category_selector = f'#ctl00_ContentView_pt{category_id}'
    category_element = retry_until_found(page.query_selector, current_category_selector)

    if not category_element:
        return records

    try:
        page.wait_for_timeout(timeout=1000)  # Wait before category click

        category_element.click()
        category_element.scroll_into_view_if_needed()  # Scroll into view
        page.wait_for_timeout(timeout=2000)  # Increase timeout

        # Update Excel file with the first category name
        category_text = category_element.inner_text()
        records.append([category_text, "", ""])

        subcategory_id = category_id + 1
        subcategory_records = process_subcategory(page, subcategory_base_selector, subcategory_id, category_id, prev_text)
        records.extend(subcategory_records)

    except Exception as e:
        print(f"Error processing category {category_id}: {str(e)}")

    return records

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
            subcategory_base_selector = '#ctl00_ContentView_pt{}'
            checkbox_base_selector = '#ctl00_ContentView_pn{}CheckBox'

            records = []
            category_id = 0
            prev_text = "0101"

            category_records = process_category(page, category_id, subcategory_base_selector, checkbox_base_selector, prev_text)
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
