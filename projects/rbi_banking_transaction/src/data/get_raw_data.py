from pathlib import Path
from requests_html import HTMLSession

datadir = Path.cwd().parent / "data"
raw_datadir = datadir / "raw"

url = "https://www.rbi.org.in/Scripts/NEFTView.aspx"

session = HTMLSession()
home = session.get(url)
session.close()


def extract_view_state(response):
    return {
        input.attrs["name"]: input.attrs["value"]
        for input in home.html.xpath(
            "//input[contains(@type, 'hidden') and contains(@name, '__')]"
        )
    }


def has_captcha(response):
    captcha_text = "This question is for testing whether you are a human visitor and to prevent automated spam submission"
    return captcha_text in " ".join(response.html.xpath("//html/body/text()"))


def post(year: str, prev_resp):
    payload = {
        "hdnYear": year,
        "hdnMonth": "0",
        "UsrFontCntr$txtSearch": "",
        "UsrFontCntr$btn": "",
    }

    view_state = extract_view_state(prev_resp)
    payload.update(view_state)
    session = HTMLSession()
    resp = session.post(url=url, data=payload)
    print(year, resp.status_code, "hasCaptcha", has_captcha(resp))
    session.close()
    return resp


years = home.html.xpath(
    "//div[contains(@class, 'accordionButton') and contains(@class, 'year')]/text()"
)
print(len(years), years[-1], years[0])


def save(file_url: str, year):
    path = raw_datadir / year
    path.mkdir(parents=True, exist_ok=True)
    dest_file = path / Path(file_url).name
    if dest_file.exists():
        return None
    print("Download", dest_file.name)
    session = HTMLSession()
    resp = session.get(file_url)
    session.close()
    with open(str(dest_file), "wb") as file:
        file.write(resp.content)


def extract_file_urls(links: list):
    return list(
        filter(lambda n: "https://rbidocs.rbi.org.in/rdocs/NEFT/DOCs" in n, links)
    )


prev_resp = home
for year in years[::-1]:
    resp = post(year, prev_resp=prev_resp)
    for url in extract_file_urls(resp.html.links):
        save(url, year)
    prev_resp = resp
