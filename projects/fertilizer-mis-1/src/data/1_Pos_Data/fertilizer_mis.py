import calendar
import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import scrapy
from scrapy import FormRequest, Request
from scrapy.crawler import CrawlerProcess


class Fertilizermisscrapper(scrapy.Spider):
    name = "fertilizermis"
    page_number = 1
    final_table = pd.DataFrame()
    project_dir = str(Path(__file__).resolve().parents[3])
    parent_folder = project_dir + "/data/raw/"
    dataset = []
    scraped_dataset = []
    nodata_available = []

    def start_requests(self):
        # this is the request that will initiate the scraping the data
        yield Request(
            "https://reports.dbtfert.nic.in/mfmsReports/getPOSReportForm"
        )

    def parse(self, response):
        "This fuction will parse the names of all states in the district and will raise another request to get all districts in a state."

        state_names = response.xpath(
            '//select[@id="parameterStateName"]/option/@value'
        ).extract()
        # print(state_names)
        for state in state_names[7:9]:
            yield FormRequest(
                url="https://reports.dbtfert.nic.in/mfmsReports/getDistrictList",
                method="POST",
                formdata={"selectedStateName": state},
                meta={"State": state},
                callback=self.dist_parser,
            )

    def dist_parser(self, response):
        "This fuction will parse the names of the districts and will raise another request for illing the dates."

        districts = json.loads(response.text)
        district_names = districts.values()
        dist_names = list(district_names)
        for dist in dist_names[1:]:
            end_date = date(2021, 5, 31)
            start_date = date(2015, 1, 1)
            delta = timedelta(days=1)
            while start_date <= end_date:
                i = start_date
                yield Request(
                    "https://reports.dbtfert.nic.in/mfmsReports/getPOSReportFormList.action?parameterFromDate={}%2F{}%2F{}&parameterDistrictName={}&d-6849390-p=1&parameterStateName={}&parameterToDate={}%2F{}%2F{}".format(
                        i.strftime("%d"),
                        i.strftime("%m"),
                        i.strftime("%Y"),
                        dist,
                        response.meta["State"],
                        i.strftime("%d"),
                        i.strftime("%m"),
                        i.strftime("%Y"),
                    ),
                    method="Post",
                    meta={
                        "State": response.meta["State"],
                        "District": dist,
                        "From Date": i.strftime("%d/%m/%Y"),
                        "To Date": i.strftime("%d/%m/%Y"),
                        "From Year": i.strftime("%Y"),
                        "From month": i.strftime("%m"),
                        "From day": i.strftime("%d"),
                        "To Year": i.strftime("%Y"),
                        "To month": i.strftime("%m"),
                        "To Day": i.strftime("%d"),
                        "End_Date": end_date.strftime("%d/%m/%Y"),
                    },
                    callback=self.get_data,
                )
                start_date += delta

    def get_data(self, response):
        "This fuction saves the csv file in the defined path."

        table = response.css("#districtTable").get()
        if table:
            table_list = pd.read_html(table)
            data = table_list[0]
            data.insert(15, "Date", response.meta["From Date"])
            current_page = response.css("strong::text").get()
            data.insert(16, "Page", current_page)
            # print(data)
            self.final_table = pd.concat([self.final_table, data], sort=False)
        else:
            self.nodata_available.append(response.meta)

        # print(self.final_table)
        pages_text = response.css("span.pagelinks > a::text").extract()
        if len(pages_text) < 1:
            pages_text = response.css(
                "span.pagelinks > strong::text"
            ).extract()
        elif pages_text[-1] != "Next ?":

            meta_data = dict(response.meta)
            print(meta_data)
            month_number = meta_data.get("From month")
            month = calendar.month_name[int(month_number)]
            file_path = (
                self.parent_folder
                + "/"
                + meta_data.get("State")
                + "/"
                + meta_data.get("District")
                + "/"
                + meta_data.get("From Year")
                + "/"
                + month
            )
            file_name = meta_data.get("From Date").replace("/", "_") + ".csv"
            self.directory(file_path)
            print(file_path + "/" + file_name)
            file_exists = file_path + "/" + file_name
            if Path(str(file_exists)).is_file():
                print("file already exists")
                pass
            else:
                self.scraped_dataset.append(meta_data)
                self.final_table.to_csv(
                    file_path + "/" + file_name, index=False
                )
                self.final_table = self.final_table.iloc[0:0]
                # print(self.final_table)

        else:
            pages_href = response.css(
                "span.pagelinks > a::attr(href)"
            ).extract()
            # print(pages_href)
            url = (
                "https://reports.dbtfert.nic.in/mfmsReports/getPOSReportFormList.action"
                + pages_href[-1]
            )
            url_match = url.split("&")
            url_match_final = url_match[2]
            # print(url_match_final)
            if url_match_final != "d-6849390-p=":
                yield Request(
                    url,
                    method="GET",
                    meta={
                        "State": response.meta["State"],
                        "District": response.meta["District"],
                        "From Date": response.meta["From Date"],
                        "From Year": response.meta["From Year"],
                        "From month": response.meta["From month"],
                    },
                    callback=self.get_data,
                )
            else:
                meta_data = dict(response.meta)
                print(meta_data)
                month_number = meta_data.get("From month")
                month = calendar.month_name[int(month_number)]
                file_path = (
                    self.parent_folder
                    + "/"
                    + meta_data.get("State")
                    + "/"
                    + meta_data.get("District")
                    + "/"
                    + meta_data.get("From Year")
                    + "/"
                    + month
                )
                file_name = (
                    meta_data.get("From Date").replace("/", "_") + ".csv"
                )
                self.directory(file_path)
                print(file_path + "/" + file_name)

                file_exists = file_path + "/" + file_name
                if Path(str(file_exists)).is_file():
                    print("file already exists")
                    pass
                else:
                    self.scraped_dataset.append(meta_data)
                    self.final_table.to_csv(
                        file_path + "/" + file_name, index=False
                    )
                    self.final_table = self.final_table.iloc[0:0]
                    # print(self.final_table)


    def directory(self, file_path):
        path_parts = file_path.split("/")
        for i in range(1, len(path_parts) + 1):
            present_path = "/".join(path_parts[:i])
            Path(present_path).mkdir(exist_ok=True)

    def closed(self, reason):
        """
        # This function saves the dataset collecting filenames to a file on the disk
        """

        print(
            "Saving all collected data[len:{}]...".format(
                len(self.scraped_dataset)
            )
        )
        json.dump(
            self.scraped_dataset,
            open(self.parent_folder + "scraped_dataset.json", "w"),
        )
        print(
            "Saving all not available data[len:{}]...".format(
                len(self.nodata_available)
            )
        )
        json.dump(
            self.nodata_available,
            open(self.parent_folder + "nodata_available.json", "w"),
        )


def main():

    process = CrawlerProcess()
    process.crawl(Fertilizermisscrapper)
    process.start()  # the script will pause here and wait for crawling to complete


if __name__ == "__main__":
    main()
