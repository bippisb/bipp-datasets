from playwright.sync_api import sync_playwright

def process_checkbox(page, checkbox_id, subcategory_base_selector, subcategory_id, prev_text, next_button_selector, export_button_selector, back_button_selector,deselect_button_selector):
    current_checkbox_selector = f'#ctl00_ContentView_pn{checkbox_id}CheckBox'
    checkbox_element = page.query_selector(current_checkbox_selector)

    if not checkbox_element:
        return

    page.wait_for_timeout(timeout=1000)
    checkbox_text_element = page.query_selector(f'#ctl00_ContentView_pt{checkbox_id}')
    checkbox_text = checkbox_text_element.inner_text()
    print(prev_text)

    if checkbox_text[:4] == prev_text[:4]:
        checkbox_element.check()
        prev_text = checkbox_text[:4]

        page.wait_for_timeout(timeout=1000)
        next_button_element = page.query_selector(next_button_selector)

        if next_button_element.is_visible():
            next_button_element.click()
            page.click(export_button_selector)
            page.click(back_button_selector)
            page.wait_for_timeout(timeout=1000)
            page.click( deselect_button_selector)


        # Increment checkbox_id after processing the current checkbox
        checkbox_id += 1
        subcategory_id+=1

        # Recursive call for the next checkbox
        process_checkbox(page, checkbox_id, subcategory_base_selector, subcategory_id, prev_text, next_button_selector, export_button_selector, back_button_selector,deselect_button_selector)
    else:
        checkbox_id += 1
        subcategory_id+=1
        prev_text = checkbox_text[:4]

        # Call the process_subcategory function
        process_subcategory(page, subcategory_id, subcategory_base_selector, prev_text, next_button_selector, export_button_selector, back_button_selector,deselect_button_selector)



def process_subcategory(page, subcategory_id, subcategory_base_selector, prev_text, next_button_selector, export_button_selector, back_button_selector,deselect_button_selector):
    current_subcategory_selector = f'#ctl00_ContentView_pt{subcategory_id}'
    subcategory_element = page.query_selector(current_subcategory_selector)

    if not subcategory_element:
        return

    subcategory_element.click()
    page.wait_for_timeout(timeout=1000)

    checkbox_id = subcategory_id+1
    process_checkbox(page, checkbox_id, subcategory_base_selector, subcategory_id, prev_text, next_button_selector, export_button_selector, back_button_selector,deselect_button_selector)


def process_category(page, category_id, prev_text, subcategory_base_selector, checkbox_base_selector, next_button_selector, export_button_selector, back_button_selector,deselect_button_selector):
    current_category_selector = f'#ctl00_ContentView_pt{category_id}'
    category_element = page.query_selector(current_category_selector)

    if not category_element:
        return

    category_element.click()
    page.wait_for_timeout(timeout=1000)
    subcategory_id=category_id+1
    process_subcategory(page, subcategory_id, subcategory_base_selector, prev_text, next_button_selector, export_button_selector, back_button_selector,deselect_button_selector)
    process_category(page, category_id, prev_text, subcategory_base_selector, checkbox_base_selector, next_button_selector, export_button_selector, back_button_selector,deselect_button_selector)

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()
        url = "http://tariffdata.wto.org/ReportersAndProducts.aspx"
        page.goto(url)

        checkbox_selector = '#ctl00_ContentView_rn304CheckBox'
        page.check(checkbox_selector)
        page.wait_for_timeout(timeout=1000)

        category_base_selector = '#ctl00_ContentView_pt{}'
        subcategory_base_selector = '#ctl00_ContentView_pt{}'
        checkbox_base_selector = '#ctl00_ContentView_pn{}CheckBox'
        next_button_selector = '#ctl00_ContentView_LinkButtonNext'
        export_button_selector = '#ctl00_ContentView_LinkButtonExport2CSV'
        back_button_selector = "#ctl00_ContentView_LinkButton1"
        deselect_button_selector="#ctl00_ContentView_PanelProductsEdit > div > table > tbody > tr:nth-child(2) > td > table > tbody > tr > td:nth-child(5) > a"

        category_id = 0
        prev_text = "0101"

        process_category(page, category_id, prev_text, subcategory_base_selector, checkbox_base_selector, next_button_selector, export_button_selector, back_button_selector,deselect_button_selector)

        input("Press Enter to close the browser...")

if __name__ == "__main__":
    main()
