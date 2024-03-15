# %%
import os
import time

from playwright.sync_api import sync_playwright

# data path
dir_path = os.path.dirname(os.path.dirname(os.getcwd()))
data_path = os.path.join(dir_path, "data")
raw_path = os.path.join(data_path, "raw")
# %%

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False, downloads_path=raw_path)
    page = browser.new_page()
    page.goto(
        "https://cimsdbie.rbi.org.in/DBIE/#/dbie/reports/Statistics/Financial%20Sector/Monetary%20Statistics",
        timeout=600000,
    )
    # wait for page to load
    page.wait_for_load_state("domcontentloaded", timeout=600000)
    mny_stk = ".table > tbody:nth-child(2) > tr:nth-child(3) > td:nth-child(1)"
    page.wait_for_selector(mny_stk, timeout=600000)
    page.locator(mny_stk).click()
    time.sleep(10)

    with page.expect_popup() as popup_info:
        popup = popup_info.value
        print("Popup opened")
    popup.wait_for_load_state("domcontentloaded", timeout=600000)
    print("Popup content loading")
    time.sleep(130)
    print("Pressing ctrl+E")
    popup.keyboard.press("Control+E")
    time.sleep(10)

    with popup.expect_popup() as popup_info:
        print("Popup opened")
        popup2 = popup_info.value
        export = "#__xmlview14--ConfirmExportButton"
        popup2.wait_for_selector(export, timeout=600000)
        popup2.locator(export).click()
        print("Download started")
    time.sleep(30)
    browser.close()
