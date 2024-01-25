# %%
from pathlib import Path
from playwright.sync_api import sync_playwright, Page
from playwright._impl._errors import TimeoutError
import re
import pandas as pd

# %%
DATA_DIR = Path(__file__).parents[1] / "data" / "raw"
URL = "http://tariffdata.wto.org/ReportersAndProducts.aspx"


# %%
def extract_do_post_back_args(href: str) -> dict[str, str]:
    pattern = r"__doPostBack\('(.*?)','(.*?)'\)"
    escaped_pattern = r"__doPostBack\(\\'(.*?)\\',\\'(.*?)\\'\)"

    matches = re.search(pattern, href)

    if matches is None:
        matches = re.search(escaped_pattern, href)

    groups = matches.groups()
    assert len(groups) == 2, "Could not extract '__doPostBack' args"

    return dict(
        __EVENTTARGET=matches.group(1),
        __EVENTARGUMENT=matches.group(2)
    )

# %%


def extract_level(p: Page, attribute_selector=""):
    root_selector = "#divProductTree a[href^='javascript:__doPostBack']"
    selector = root_selector + attribute_selector + ":has(img)"
    items = p.locator("css=" + selector).all()
    return {
        item.get_by_role("img").get_attribute("alt"): extract_do_post_back_args(item.get_attribute("href"))
        for item in items
    }


# %%
def extract_level_3(p: Page, parent_code: str):
    selector = "td span"
    items = p.locator("css=" + selector).filter(
        has_text=parent_code
    ).all()

    return [
        item.inner_text()
        for item in items
    ]


def wait_for_page_to_finish_loading(p: Page):
    try:
        p.wait_for_function(
            '() => document.querySelector("#ctl00_ContentView_UpdateProgressObject").style.display == "block"')
    except TimeoutError:
        pass
    p.wait_for_function(
        '() => document.querySelector("#ctl00_ContentView_UpdateProgressObject").style.display == "none"')


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(URL)
        page.wait_for_load_state()

        editions = page.locator(
            "css=table#ctl00_ContentView_nomenclatureDropDownList input").all()
        for edition in [list(editions)[-1]]:
            edition_title = edition.get_attribute("value")
            print(edition_title)

            if not edition.is_checked():
                page.locator(
                    "css=#ctl00_ContentView_lblNomenclature").first.click()
                edition.wait_for(state="visible")
                edition.click()
                wait_for_page_to_finish_loading(page)

            hs_codes = list()
            l1 = extract_level(page)
            print(len(l1))
            for l1_title, args in l1.items():
                img = page.get_by_alt_text(l1_title)
                img.wait_for(state="visible")
                img.click()

                wait_for_page_to_finish_loading(page)

                attr_selector = f"[href*='{args['__EVENTARGUMENT']}\\\\']"
                l2 = extract_level(page, attribute_selector=attr_selector)
                print("\t", len(l2))
                for l2_title, args in l2.items():
                    img = page.get_by_alt_text(l2_title)
                    img.wait_for(state="visible")
                    img.click()

                    wait_for_page_to_finish_loading(page)

                    parent_code = args["__EVENTARGUMENT"].split("\\")[-1]
                    l3_titles = extract_level_3(page, parent_code)
                    print("\t", parent_code, len(l3_titles))
                    hs_codes.append([l1_title, l2_title, l3_titles])

            df = pd.DataFrame(hs_codes, columns=["l1", "l2", "l3"])
            df.to_csv(DATA_DIR / f"{edition_title}.csv")


# %%
if __name__ == "__main__":
    main()
