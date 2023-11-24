# %%
import binascii
from core import API, Globals
from core import save_json
import httpx
import polars as pl
from bs4 import BeautifulSoup as Soup
from bs4 import Tag
import re
from functools import partial
import base64


# %%


def clean_text(raw_text: str):
    pattern = r'[\n\r\t]'
    text = re.sub(pattern, '', raw_text)
    text = text.strip(" :")
    return text

# %%


def parse_particular(t: Tag):
    field = clean_text(t.select_one("b.font-15").text)
    value = clean_text(t.select_one("span.pFont14").text)
    return field, value


# %%
def parse_school_profile(s: Soup):
    rows = s.select(
        "div.card-header:has(h5:contains('School Profile')) + div.card-block .row .row")
    return dict(parse_particular(row) for row in rows)


# %%
def select_accordian_content(soup: Soup, title: str):
    return soup.select_one(f".card-header:has(.card-title:contains('{title}')) + .card-body")

# %%


def parse_accordian(*, soup: Soup, title: str):
    content = select_accordian_content(soup, title)
    if content is None:
        return None
    rows = content.select(".row .row")
    return dict(parse_particular(row) for row in rows)


# %%
parse_basic_details = partial(
    parse_accordian, title="Basic Details")
parse_facilities = partial(parse_accordian, title="Facilities")
parse_room_details = partial(
    parse_accordian, title="Room Details")


# %%
def extract_report_card_args(text: str):
    pattern = r'\(([^)]*)\)'
    match = re.search(pattern, text)

    assert match, f"Report card args not found in '{text}'"

    args = list(map(str.strip, match.group(1).split(",")))
    assert len(args) == 2, f"Unexpected number of args in '{text}'"

    return dict(
        schoolId=args[0],
        yearId=args[1]
    )
# %%


def parse_report_card_buttons(s: Soup):
    buttons = s.select("ul:has(h5:contains('Report Card')) button")
    return [
        dict(
            year=clean_text(btn.text),
            **extract_report_card_args(btn.attrs["onclick"])
        )
        for btn in buttons
    ]


# %%
def get_report_card(client: httpx.Client, schoolId: str, yearId: str):
    r = client.post(API.REPORT_CARD.value, data={
        "schoolId": schoolId,
        "yearId": yearId,
    })
    return r.content


def save_base64_pdf(base64_pdf: str, path: str):
    pdf = base64.b64decode(base64_pdf)
    with open(path, "wb") as f:
        f.write(pdf)


def download_report_cards(client: httpx.Client, soup: Soup):
    buttons = parse_report_card_buttons(soup)
    for button in buttons:
        base64_pdf = get_report_card(
            client, button["schoolId"], button["yearId"])
        save_base64_pdf(base64_pdf, button["year"] + ".pdf")
# %%


def parse_student_enrolment(s: Soup):
    content = select_accordian_content(s, "Enrolment")
    if content is None:
        return None
    no_of_students_td = content.select(
        "tr:has(th:contains('No. of Students')) td")
    classes_td = content.select("tr:has(th:contains('Class')) td")
    return [
        {
            "class": clean_text(_class.text),
            "no_of_students": clean_text(no_of_students.text)
        }
        for _class, no_of_students in zip(classes_td, no_of_students_td)
    ]

# %%


def parse_total_teachers(s: Soup):
    content = select_accordian_content(s, "Enrolment")
    if content is None:
        return None
    row = content.select("tr:has(td:contains('Total Teachers')) td")
    return [clean_text(td.text) for td in row]


# %%
dest_dir = Globals.SCHOOLS_DIR.value.relative_to(
    Globals.DATA_DIR.value).as_posix()
derive_destination_path = pl.concat_str([
    pl.lit(dest_dir),
    pl.col("stateId"),
    pl.col("districtId"),
    pl.col("blockId"),
    pl.col("schoolId")
], separator="/")
# %%


def path_exists(dest_dir: str):
    return (Globals.DATA_DIR.value / dest_dir).exists()


# %%
def scrape(school: dict, client=httpx.Client()):
    r = client.post(
        API.SCHOOL_DETAIL.value,
        data=dict(schoolIdforDashSearch=school["schoolId"]),
        timeout=20.0
    )
    dest_dir = Globals.DATA_DIR.value / school["dest_dir"]
    dest_dir.mkdir(parents=True, exist_ok=True)
    if r.content == b"":
        return 0

    soup = Soup(r.text, features="html.parser")
    data = {
        "basic_details": parse_basic_details(soup=soup),
        "school_profile": parse_school_profile(soup),
        "facilities": parse_facilities(soup=soup),
        "room_details": parse_room_details(soup=soup),
        "student_enrolment": parse_student_enrolment(soup),
        "total_teachers": parse_total_teachers(soup),
        "report_cards": parse_report_card_buttons(soup),
    }

    save_json(data, dest_dir / "school_details.json")

    for report_card in data["report_cards"]:
        r = client.post(API.REPORT_CARD.value, data={
            "schoolId": report_card["schoolId"],
            "yearId": report_card["yearId"],
        })
        try:
            save_base64_pdf(r.content, dest_dir / (report_card["year"] + ".pdf"))
        except binascii.Error:
            continue
    return 1

# %%


def main():
    index = pl.read_parquet(Globals.SCHOOL_INDEX_DIR.value / "schools.parquet")
    index = index.with_columns(derive_destination_path.alias("dest_dir"))

    # school details are not available for the following
    # permanently closed schools (5-Permanently Closed)
    # merged (2-Merged)
    index = index.filter(~pl.col("schoolStatus").is_in([2, 5]))
    scrapables = index.filter(
        pl.col("dest_dir").map_elements(lambda x: not path_exists(x)))

    client = httpx.Client()
    scraper = partial(scrape, client=client)
    for item in scrapables.iter_rows(named=True):
        # try:
        print("Scraping", "state", item["stateName"], "district", item["districtName"], "block", item["blockName"], "schoolId", item["schoolId"], "udiseSchCode",
              item["udiseSchCode"], "schoolStatus", item["schoolStatus"])
        status_code = scraper(item)
        if status_code == 1:
            print("\tSUCCESS")
        elif status_code == 0:
            print("\tSchool Details Unavailable")


if __name__ == "__main__":
    main()
