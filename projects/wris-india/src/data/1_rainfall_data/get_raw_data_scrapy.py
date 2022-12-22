# import datetime
import json
from pathlib import Path

# import pandas as pd
import scrapy
from scrapy import Request
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


class RainfallWris(scrapy.Spider):
    name = "rainfallwris"
    dataset_name = "Rainfall"
    state_data = list()
    district_data = list()
    station_data = list()
    project_dir = str(Path(__file__).resolve().parents[3])
    parent_folder = project_dir + "/data/raw/"
    print(parent_folder)

    def __init__(self):
        self.input = input  # source file name
        print("NOTE: Enter yyyymmdd format for only current year data")
        self.start_year_input = input(
            "Enter the start year to download the data in yyyy or yyyymmdd format:"
        )
        self.end_year_input = input("Enter the end year in yyyy or yyyymmdd format:")

    def start_requests(self):
        # this is the request that will initiate the scraping the data
        yield Request("https://wdo.indiawris.gov.in/api/comm/src/rainfall_report")

    def parse(self, response):

        source_list = json.loads(response.text)

        for source in source_list:
            if source["value"] == "ALL AGENCIES":
                # Getting the vallues for all the states
                state_payload = {
                    "pType": "COUNTRY",
                    "cType": "STATE",
                    "component": "rainfall",
                    "src": "STATE_AND_CENTRAL_STATION",
                }
                district_payload = {
                    "pType": "STATE",
                    "cType": "DISTRICT",
                    "component": "rainfall",
                    "src": "STATE_AND_CENTRAL_STATION",
                }
                station_payload = {
                    "pType": "DISTRICT",
                    "cType": "STATION",
                    "component": "rainfall",
                    "src": "STATE_AND_CENTRAL_STATION",
                }
                state_values_payload = json.dumps(state_payload)
                district_values_payload = json.dumps(district_payload)
                station_values_payload = json.dumps(station_payload)

                headers = {
                    "Content-Type": "application/json",
                    "Referer": "https://wdo.indiawris.gov.in/waterdataonline/analysis;selectedsidebar=downloadreport;component=All",
                }
                yield Request(
                    url="https://wdo.indiawris.gov.in/api/locations/allChildrenForParentChildType",
                    method="POST",
                    callback=self.state_parser,
                    body=state_values_payload,
                    headers=headers,
                )
                yield Request(
                    url="https://wdo.indiawris.gov.in/api/locations/allChildrenForParentChildType",
                    method="POST",
                    callback=self.district_parser,
                    body=district_values_payload,
                    headers=headers,
                )
                yield Request(
                    url="https://wdo.indiawris.gov.in/api/locations/allChildrenForParentChildType",
                    method="POST",
                    callback=self.station_parser,
                    body=station_values_payload,
                    headers=headers,
                )

    def state_parser(self, response):
        # print(response.text)
        print("State Data")
        self.state_data = json.loads(response.text)
        # print(self.state_data)

    def district_parser(self, response):
        # print(response.text)
        self.district_data = json.loads(response.text)
        print("District Data")
        # print(self.district_data)

    def station_parser(self, response):
        self.station_data = json.loads(response.text)
        print("Station Data")
        for state in self.state_data["INDIA"]:
            print("State ", state)
            state_name = state["name"]
            state_uid = state["uuid"]
            for district_entity in self.district_data[state["name"]]:
                district_name = district_entity["name"]
                district_uid = district_entity["uuid"]
                try:
                    for station_entity in self.station_data[district_name]:
                        print("Station " * 5)
                        # print(station_entity["name"])
                        # print(station_entity["uuid"])
                        # print(station_entity["sc"])
                        station_name = station_entity["name"]
                        # station_sc = station_entity["sc"]
                        station_uid = station_entity["uuid"]

                        headers = {
                            "Content-Type": "application/json",
                            "Referer": "https://wdo.indiawris.gov.in/waterdataonline/analysis;selectedsidebar=downloadreport;component=All",
                        }

                        # today_date = date.today()
                        # for current year, no iteration over years.
                        # print("Given Start Date is ,", self.start_year_input)
                        # print("Given End Date is ,", self.end_year_input)
                        if len(str(self.start_year_input)) == 8:
                            station_complete_values = {
                                "lType": "STATION",
                                "src": "STATE_AND_CENTRAL_STATION",
                                "view": "ADMIN",
                                "aggr": "SUM",
                                "reportType": "Station Wise Timeseries",
                                "sDate": self.start_year_input,
                                "eDate": self.end_year_input,
                                "fileformat": "xls",
                                "calendarFormat": "yyyyMMdd",
                                "STATE": [state_uid],
                                "DISTRICT": [district_uid],
                                "STATION": [station_uid],
                                "stationType": "ALL",
                            }
                            station_complete_payload = json.dumps(
                                station_complete_values
                            )
                            # yielding a request to download the file.
                            yield Request(
                                url="https://wdo.indiawris.gov.in/api/rf/report",
                                method="POST",
                                callback=self.file_downloader,
                                body=station_complete_payload,
                                headers=headers,
                                meta={
                                    "state_name": state_name,
                                    "district_name": district_name,
                                    "station_name": station_name,
                                    "start_year": station_complete_values["sDate"],
                                    "end_year": station_complete_values["eDate"],
                                },
                            )

                        else:
                            self.start_year_input = int(self.start_year_input)
                            self.end_year_input = int(self.end_year_input)
                            # this condition is to get historical data until 2020.
                            for year in range(
                                self.start_year_input, self.end_year_input + 1
                            ):
                                print("date compilation")
                                start_date_value = str(year) + "0101"
                                end_date_value = str(year) + "1231"
                                station_complete_values = {
                                    "lType": "STATION",
                                    "src": "STATE_AND_CENTRAL_STATION",
                                    "view": "ADMIN",
                                    "aggr": "SUM",
                                    "reportType": "Station Wise Timeseries",
                                    "sDate": start_date_value,
                                    "eDate": end_date_value,
                                    "fileformat": "xls",
                                    "calendarFormat": "yyyyMMdd",
                                    "STATE": [state_uid],
                                    "DISTRICT": [district_uid],
                                    "STATION": [station_uid],
                                    "stationType": "ALL",
                                }
                                station_complete_payload = json.dumps(
                                    station_complete_values
                                )
                                # yielding a request to download the file.
                                yield Request(
                                    url="https://wdo.indiawris.gov.in/api/rf/report",
                                    method="POST",
                                    callback=self.file_downloader,
                                    body=station_complete_payload,
                                    headers=headers,
                                    meta={
                                        "state_name": state_name,
                                        "district_name": district_name,
                                        "station_name": station_name,
                                        "start_year": station_complete_values["sDate"],
                                        "end_year": station_complete_values["eDate"],
                                    },
                                )

                except KeyError:
                    print(
                        "ERROR: district not found ",
                        district_name,
                        state_name,
                    )
                    print("No Stations Available in this District")
                    # Write a logic to note down all the districts where there are no stations available.
        pass

    def file_downloader(self, response):

        path = Path(
            self.parent_folder
            + "/"
            + "1_rainfall_data"
            + "/"
            + response.meta["state_name"]
            + "/"
            + response.meta["district_name"]
            + "/"
            + response.meta["start_year"]
            + "_"
            + response.meta["end_year"]
            + "/"
        )

        path.mkdir(parents=True, exist_ok=True)
        # unquote() cleans up the reserved characters in the url to create a clean file name
        filename = (
            str(path)
            + "/"
            + response.meta["start_year"]
            + "_"
            + response.meta["end_year"]
            + ".xlsx"
        )
        # print(type(response.body))
        with open(filename, "wb") as f:
            f.write(response.body)

    # custom_settings = {
    #     DOWNLOADER_MIDDLEWARES :{
    #         'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    #         'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
    #         'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 400,
    #         'scrapy_fake_useragent.middleware.RetryUserAgentMiddleware': 401,
    #     }
    # }


def main():

    settings = get_project_settings()
    settings.set("CUSTOM_SETTING", "Super Custom Setting")
    settings.update(
        {
            # "CONCURRENT_REQUESTS": 1,
            "ROBOTSTXT_OBEY": True,
            # "AUTOTHROTTLE_ENABLED": True,
            # "DOWNLOAD_DELAY": 1.5,
            "BOT_NAME": "rainfallwris",
            # "DOWNLOADER_MIDDLEWARES": {
            #     "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": 390,
            #     "scrapy.downloadermiddlewares.retry.RetryMiddleware": 391,
            #     "scrapy_fake_useragent.middleware.RandomUserAgentMiddleware": 400,
            #     "scrapy_fake_useragent.middleware.RetryUserAgentMiddleware": 401,
            # },
        }
    )
    process = CrawlerProcess(settings)
    process.crawl(RainfallWris)
    process.start()  # the script will pause here and wait for crawling to complete


if __name__ == "__main__":
    main()
