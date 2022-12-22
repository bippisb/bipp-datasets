from pathlib import Path

import pandas as pd
import scrapy
from scrapy import FormRequest, Request
from scrapy.crawler import CrawlerProcess


class psdataNagalandscraper(scrapy.Spider):
    name = "psdataNagaland"

    custom_settings = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.128 Safari/537.36"
    }

    project_dir = str(Path(__file__).resolve().parents[3])
    parent_folder = project_dir + "/data/raw/"

    def start_requests(self):
        # request to initiate the scraping
        yield Request("http://ceo.nagaland.gov.in/DownloadERoll")

    def parse(self, response):
        # "This fuction will parse the names of all the districts in the state and will raise another request to get all ACS in the district."

        dist_names = response.xpath(
            '//*[@id="ContentPlaceHolder1_DropDownListDistrict"]/option/text()'
        ).extract()
        dist_values = response.xpath(
            '//*[@id="ContentPlaceHolder1_DropDownListDistrict"]/option/@value'
        ).extract()
        print(dist_names)
        print(dist_values)
        # print(response.text)
        i = 1
        form_dict = {
            "ctl00$ctl08": "ctl00$ctl08|ctl00$ContentPlaceHolder1$DropDownListDistrict",
            "__EVENTTARGET": "ctl00$ContentPlaceHolder1$DropDownListDistrict",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": response.css("#__VIEWSTATE::attr(value)").extract_first(),
            "__VIEWSTATEGENERATOR": response.css(
                "#__VIEWSTATEGENERATOR::attr(value)"
            ).extract_first(),
            "__EVENTVALIDATION": response.css(
                "#__EVENTVALIDATION::attr(value)"
            ).extract_first(),
            "ctl00$ContentPlaceHolder1$DropDownListAC": "Select AC...",
            "ctl00$ContentPlaceHolder1$DropDownListPart": "Select Part...",
            "_ASYNCPOST": "true",
        }
        print(form_dict)

        for dist in dist_values[1:]:
            form_dict["ctl00$ContentPlaceHolder1$DropDownListDistrict"] = dist
            yield FormRequest.from_response(
                response,
                url="http://ceo.nagaland.gov.in/DownloadERoll",
                method="POST",
                formdata=form_dict,
                # dont_click="True",
                callback=self.ac_parser,
                meta={"district_code": dist},
            )
            i += 1

    def ac_parser(self, response):
        # print(response.text)
        # This function gets the list assembly constituencies and raises another request to get ps_data of each constituency.
        ac_list = response.xpath(
            '//select[@id="ContentPlaceHolder1_DropDownListAC"]/option/text()'
        ).extract()
        ac_values = response.xpath(
            '//select[@id="ContentPlaceHolder1_DropDownListAC"]/option/@value'
        ).extract()

        print(ac_list)
        print(ac_values)
        i = 1
        form_dict = {
            "ctl00$ctl08": "ctl00$ContentPlaceHolder1$UPAC|ctl00$ContentPlaceHolder1$DropDownListAC",
            "__EVENTTARGET": "ctl00$ContentPlaceHolder1$DropDownListAC",
            "__EVENTARGUMENT": response.css(
                "#__EVENTARGUMENT::attr(value)"
            ).extract_first(),
            "__LASTFOCUS": response.css("#__LASTFOCUS::attr(value)").extract_first(),
            "__VIEWSTATE": response.css("#__VIEWSTATE::attr(value)").extract_first(),
            "__VIEWSTATEGENERATOR": response.css(
                "#__VIEWSTATEGENERATOR::attr(value)"
            ).extract_first(),
            "__EVENTVALIDATION": response.css(
                "#__EVENTVALIDATION::attr(value)"
            ).extract_first(),
            "ctl00$ContentPlaceHolder1$DropDownListDistrict": response.meta[
                "district_code"
            ],
            "ctl00$ContentPlaceHolder1$DropDownListPart": "Select Part...",
            "_ASYNCPOST": "true",
        }
        for ac in ac_values[1:]:
            form_dict["ctl00$ContentPlaceHolder1$DropDownListAC"] = ac
            yield FormRequest.from_response(
                response,
                url="http://ceo.nagaland.gov.in/DownloadERoll",
                method="POST",
                formdata=form_dict,
                meta={"ac_names": ac_list[i]},
                # dont_click="True",
                callback=self.save_data,
            )
            i += 1

    def save_data(self, response):
        # This function gets list of psdata for all the constituencies and saves the data.
        final_table = response.xpath(
            '//select[@id="ContentPlaceHolder1_DropDownListPart"]/option/text()'
        ).extract()
        print(final_table)
        table_list = pd.DataFrame(final_table, columns=["Polling_Station_Name"])
        table_list = table_list.iloc[1:, 0:]

        file_path = (
            self.parent_folder + "/" + "4_Nagaland" + "/" + response.meta["ac_names"]
        )
        file_name = response.meta["ac_names"] + ".csv"
        self.directory(file_path)
        table_list.to_csv(file_path + "/" + file_name, index=False)

    def directory(self, file_path):
        # This function creates directory and appropriate file path to save the data.
        path_parts = file_path.split("/")
        for i in range(1, len(path_parts) + 1):
            present_path = "/".join(path_parts[:i])
            Path(present_path).mkdir(exist_ok=True)


def main():
    process = CrawlerProcess()
    # process = CrawlerProcess({
    # 'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.128 Safari/537.36'})
    process.crawl(psdataNagalandscraper)
    process.start()  # the script will pause here and wait for crawling to complete


if __name__ == "__main__":
    main()
