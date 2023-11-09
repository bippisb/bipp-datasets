import scrapy
import pandas as pd
from scrapy.http import FormRequest
from datetime import datetime
from pathlib import Path
import os

class PricesSpider(scrapy.Spider):
    name = 'prices'
    start_urls = ['https://fcainfoweb.nic.in/reports/report_menu_web.aspx']

    def parse(self, response):
        data = {
            'ctl00_MainContent_ToolkitScriptManager1_HiddenField': '',
            'ctl00$MainContent$Ddl_Rpt_type': 'Retail',
            'ctl00$MainContent$ddl_Language': 'English',
            'ctl00$MainContent$Rbl_Rpt_type': 'Price report',
        }
        yield FormRequest.from_response(response, formdata=data, callback=self.step2, dont_filter=True)

    def step2(self, response):
        data = {
            'ctl00$MainContent$Ddl_Rpt_Option0': 'Daily Prices'
        }
        yield FormRequest.from_response(response, formdata=data, callback=self.step3, dont_filter=True)

    def step3(self, response):
        start_date = datetime(2023, 11, 1)
        end_date = datetime(2023, 11, 3)  # Change end_date as needed
        current_date = start_date
        while current_date <= end_date:
            formatted_date = current_date.strftime('%d/%m/%Y')
            data = {
                'ctl00$MainContent$Txt_FrmDate': formatted_date,
                'ctl00$MainContent$btn_getdata1': 'Get Data'
            }
            yield FormRequest.from_response(response, formdata=data, callback=self.parse_table, meta={'current_date': formatted_date}, dont_filter=True)
            current_date += pd.Timedelta(days=1)

    def parse_table(self, response):
        current_date_str = response.meta.get('current_date')  # Get the date as a string
        current_date = datetime.strptime(current_date_str, '%d/%m/%Y')  # Convert it to datetime object

        table = response.css('#gv0')  
        if table:
            df = pd.read_html(table.extract_first(), header=0)[0]  

            formatted_date = current_date.strftime("%d-%m-%Y")
            self.log(f"Saving data for {formatted_date}")
            self.save_data(df, formatted_date)
        else:
            self.log(f"No table found for {current_date_str}")

    def save_data(self, df, formatted_date):
        new_filename = f"retail_prices_{formatted_date}.csv"
        try:
            base_path = Path("../data/raw")
            os.makedirs(base_path, exist_ok=True)
            output_path = base_path / new_filename
            df.to_csv(output_path, index=False, encoding='utf-8')
        except OSError as file_error:
            self.log(f"File operation error: {file_error}")
        except Exception as e:
            self.log(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    from scrapy.crawler import CrawlerProcess

    process = CrawlerProcess()

    process.crawl(PricesSpider)
    process.start()
