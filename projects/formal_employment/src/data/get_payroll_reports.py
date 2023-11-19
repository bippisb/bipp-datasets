"""
The script downloads all the payroll reports released on MOSPI website.
"""

import scrapy
from pathlib import Path
import urllib.parse


def prep_download_path(filename: str):
    data_dir = Path.cwd() / "raw"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir / filename


class GetPayrollReportsSpider(scrapy.Spider):
    name = "get_payroll_reports"
    allowed_domains = ["mospi.gov.in"]
    start_urls = ["https://www.mospi.gov.in/press-release"]

    def parse(self, response):
        for a in response.css(".pressReleaseTitle a"):
            if "Payroll Reporting in India" not in a.attrib["title"]:
                continue
            yield scrapy.Request(a.attrib["href"], callback=self.download_report)

        next_page = response.css(
            ".pager-current + .pager-item a::attr('href')").get()
        if next_page is not None:
            yield response.follow(next_page, self.parse)

    def download_report(self, response):
        filename = urllib.parse.unquote(response.url.split("/")[-1])
        with open(prep_download_path(filename), 'wb') as f:
            f.write(response.body)
        self.log('Saved file %s' % filename)
