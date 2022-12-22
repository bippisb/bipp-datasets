import hashlib
import json
from pathlib import Path

import pandas as pd
import scrapy
from scrapy import FormRequest, Request
from scrapy.crawler import CrawlerProcess


class PmgsyScraper(scrapy.Spider):
    def __init__(self):
        self.name = "pmgsyscraper"
        super().__init__()

        self.project_code = "PaFPS"
        self.project_link = "PhysicalFinancialMonitoringTwo"  # for basic report: PhysicalAndFinancialProjectSummary
        self.project_name = "2_physical-and-financial-project-summary"

        self.project_dir = str(Path(__file__).resolve().parents[3])
        # print(self.project_dir)
        self.parent_folder = (
            self.project_dir + "/data/raw/" + self.project_name + "/"
        )
        self.output_dir = self.parent_folder + "output_files/"
        self.dataset = []
        self.failed_requests = []

        self.ensure_directory(self.parent_folder)
        self.ensure_directory(self.output_dir)

    def start_requests(self):
        # this is the request that will initiate the layout for this report
        # similarly for other reports there will be similar kind of request being sent.
        yield Request(
            "http://omms.nic.in/ProposalArea/Proposal/PhyFinMonitorLayout?_=1623910664919",
            errback=self.err_handler,
        )

    def parse(self, response):
        """
        This function will parse the details of all the states present in country,
        and raises another request to get all the districts.
        """
        # all these state code and names, year value and text need to converted as key value pair
        # and be called using dictionaries.
        state_codes = response.xpath(
            '//*[@id="StateList_PhyFinMonitorDetails"]/option/@value'
        ).extract()
        state_names = response.xpath(
            '//*[@id="StateList_PhyFinMonitorDetails"]/option/text()'
        ).extract()
        state_names = [state.replace(" ", "+") for state in state_names]
        year_list_values = response.xpath(
            '//*[@id="YearList_PhyFinMonitorDetails"]/option/@value'
        ).extract()
        year_list_text = response.xpath(
            '//*[@id="YearList_PhyFinMonitorDetails"]/option/text()'
        ).extract()
        batch_list_values = response.xpath(
            '//*[@id="BatchList_PhyFinMonitorDetails"]/option/@value'
        ).extract()
        batch_list_text = response.xpath(
            '//*[@id="BatchList_PhyFinMonitorDetails"]/option/text()'
        ).extract()
        colab_list_values = response.xpath(
            '//*[@id="FundingAgencyList_PhyFinMonitorDetails"]/option/@value'
        ).extract()
        colab_list_text = response.xpath(
            '//*[@id="FundingAgencyList_PhyFinMonitorDetails"]/option/text()'
        ).extract()

        # converting lists to dictionaries
        state_dict = dict(zip(state_codes, state_names))
        year_dict = dict(zip(year_list_values, year_list_text))
        batch_dict = dict(zip(batch_list_values, batch_list_text))
        colab_dict = dict(zip(colab_list_values, colab_list_text))
        # looping through the states to get the districts,
        # we are starting from one since '0' is 'select state'
        for state_code in state_codes[1:]:
            yield FormRequest(
                url="http://omms.nic.in/NationalArea/National/DistrictDetails",
                method="POST",
                formdata={"StateCode": state_code},
                meta={
                    "state_code": state_code,
                    "state_name": state_dict[state_code],
                    "year_dict": year_dict,
                    "batch_dict": batch_dict,
                    "colab_dict": colab_dict,
                },
                callback=self.district_parser,
                errback=self.err_handler,
            )

    def district_parser(self, response):
        """
        This function will parse the details of all the districts present in a state,
        and raises another request to get all the blocks in a district
        """
        # print(type(response.text))
        district_list = json.loads(response.text)
        # print(district_list)
        # looping through the districts to get the blocks,
        # we are starting from one since '0' is 'all districts'
        for dist in district_list[1:]:
            dist_code = dist["Value"]
            dist_name = dist["Text"].replace(" ", "+")
            yield FormRequest(
                url="http://omms.nic.in/NationalArea/National/BlockDetails",
                method="POST",
                formdata={
                    "StateCode": response.meta["state_code"],
                    "DistrictCode": dist_code,
                },
                meta={
                    "state_code": response.meta["state_code"],
                    "dist_code": dist_code,
                    "dist_name": dist_name,
                    "state_name": response.meta["state_name"],
                    "year_dict": response.meta["year_dict"],
                    "batch_dict": response.meta["batch_dict"],
                    "colab_dict": response.meta["colab_dict"],
                },
                callback=self.block_parser,
                errback=self.err_handler,
            )

    def block_parser(self, response):
        """
        This function will parse the details of all the blocks present in a district,
        and raises another request to get the data for combination of years, batches,
        and collaborations for each block, district and state.
        """
        # print(response.text)
        block_list = json.loads(response.text)

        for block in block_list[1:]:
            block_code = block["Value"]
            block_text = block["Text"]

            meta = {
                "state_code": response.meta["state_code"],
                "state_name": response.meta["state_name"],
                "dist_code": response.meta["dist_code"],
                "dist_name": response.meta["dist_name"],
                "block_code": block_code,
                "block_name": block_text,
                "year_dict": response.meta["year_dict"],
                "batch_dict": response.meta["batch_dict"],
                "colab_dict": response.meta["colab_dict"],
            }
            year = "0"
            # for year in meta["year_dict"]:
            #

            # batch_code = "0"
            # batch_name = "All+Batches"
            # colab_name = "All+Collaborations"
            # colab_code = "0"
            for batch_code in meta["batch_dict"]:
                batch_name = meta["batch_dict"][batch_code]
                for colab_code in meta["colab_dict"]:
                    colab_name = meta["colab_dict"][colab_code]
                    yield Request(
                        url="http://omms.nic.in/MvcReportViewer.aspx?_r=%2fPMGSYCitizen%2f{}&Level=4&State={}&District={}&Block={}&Year={}&Batch={}&Collaboration={}&PMGSY=1&Status=%25&LocationName={}&DistrictName={}&BlockName={}&LocalizationValue=en&BatchName={}&CollaborationName={}".format(
                            self.project_link,
                            meta["state_code"],
                            meta["dist_code"],
                            meta["block_code"],
                            year,
                            batch_code,
                            colab_code,
                            meta["state_name"],
                            meta["dist_name"],
                            meta["block_name"],
                            batch_name,
                            colab_name,
                        ),
                        method="POST",
                        meta={
                            "state_code": meta["state_code"],
                            "state_name": meta["state_name"],
                            "dist_code": meta["dist_code"],
                            "dist_name": meta["dist_name"],
                            "block_code": meta["block_code"],
                            "block_name": meta["block_name"],
                            "year": year,
                            "year_dict": response.meta["year_dict"],
                            "batch_code": batch_code,
                            "batch_name": batch_name,
                            "colab_code": colab_code,
                            "colab_name": colab_name,
                        },
                        callback=self.data_collector,
                        errback=self.err_handler,
                    )

    def data_collector(self, response):
        """
        This function extracts the data from the table in the HTML and saves it
        to the disk.
        """
        meta_data = dict(response.meta)
        file_path = (
            self.parent_folder
            + "/"
            + str(meta_data["state_name"])
            + "/"
            + str(meta_data["dist_name"])
            + "/"
            + str(meta_data["block_name"])
            + "/"
            + str(meta_data["year_dict"][meta_data["year"]])
        )
        file_name = (
            meta_data["batch_name"] + "_" + meta_data["colab_name"] + ".csv"
        )
        final_file_name = file_path + "/" + file_name

        if Path(str(final_file_name)).is_file():
            print("file already exists")
            pass
        else:
            table = response.css(
                "#ReportViewer_ctl09_ReportControl div div table tr td table "
            ).get()
            print("GETTING THE DATA NOW " * 5)
            table_list = pd.read_html(table)
            # print(table_list)
            road_data = table_list[7]
            print(road_data)
            # road_data = pd.DataFrame()

        meta_data["filename"] = None
        if road_data.shape[0] > 4:
            file_path = self.output_dir
            file_name = hashlib.md5(
                json.dumps(meta_data).encode("utf8")
            ).hexdigest()[:15]
            road_data.to_csv(file_path + file_name + ".csv")
            meta_data["filename"] = file_name

            file_path = (
                self.parent_folder
                + "/"
                + str(meta_data["state_name"])
                + "/"
                + str(meta_data["dist_name"])
                + "/"
                + str(meta_data["block_name"])
                + "/"
                + str(meta_data["year_dict"][meta_data["year"]])
            )
            file_name = (
                meta_data["batch_name"]
                + "_"
                + meta_data["colab_name"]
                + ".csv"
            )
            self.ensure_directory(file_path)
            road_data.to_csv(file_path + "/" + file_name)
            meta_data["filename"] = None
            if road_data.shape[0] > 4:
                file_path = self.output_dir
                file_name = hashlib.md5(
                    json.dumps(meta_data).encode("utf8")
                ).hexdigest()[:15]
                road_data.to_csv(file_path + file_name + ".csv")
                meta_data["filename"] = file_name

                file_path = (
                    self.parent_folder
                    + "/"
                    + str(meta_data["state_name"])
                    + "/"
                    + str(meta_data["dist_name"])
                    + "/"
                    + str(meta_data["block_name"])
                    + "/"
                    + str(meta_data["year_dict"][meta_data["year"]])
                )
                file_name = (
                    meta_data["batch_name"]
                    + "_"
                    + meta_data["colab_name"]
                    + ".csv"
                )
                self.ensure_directory(file_path)
                road_data.to_csv(file_path + "/" + file_name)

            self.dataset.append(meta_data)

    def ensure_directory(self, file_path):
        """
        This function ensures the directory path provided in file_path
        exists, if not creates the path
        """

        path_parts = file_path.split("/")
        for i in range(1, len(path_parts) + 1):
            present_path = "/".join(path_parts[:i])
            Path(present_path).mkdir(exist_ok=True)

    def err_handler(self, response):
        """
        This function handles any error that occur during the processing of any
        request
        """

        self.failed_requests.append(
            (dict(response.meta), response.request.url)
        )

    def closed(self, reason):
        """
        This function saves the dataset collecting filenames to a file on the disk
        """

        print("Saving all collected data[len:{}]...".format(len(self.dataset)))
        json.dump(
            self.dataset,
            open(self.parent_folder + "scraped_dataset.json", "w"),
        )

        if self.failed_requests:
            print("Saving all errs[len:{}]".format(len(self.failed_requests)))
            json.dumps(
                self.failed_requests,
                open(self.parent_folder + "failed_requests.json", "w"),
            )


def main():
    """
    This is the script to scrape raw data from PMGSY website for Physical Monitoring of Works dataset.

    USAGE: python get_pmgsy_data.py
    """
    # Create a Crawler Process with custom settings
    process = CrawlerProcess()
    process.crawl(PmgsyScraper)
    process.start()  # the script will block here until the crawling is finished


if __name__ == "__main__":
    main()
