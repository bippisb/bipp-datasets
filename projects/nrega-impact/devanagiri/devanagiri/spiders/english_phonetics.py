import scrapy


class english_spider(scrapy.Spider):
    name = "english"

    start_urls = ["https://www.easyhindityping.com/hindi-alphabet"]

    def parse(self, response):
        self.logger.info("Scrapper for Devangiri - English dictionary")
        yield {
            "devan_text": response.xpath(
                "//div[contains(@class, 'native hindi-native')]/text()"
            ).getall(),
            "english_phonetics": response.xpath(
                "//div[contains(@class, 'latin hindi-latin')]/text()"
            ).getall(),
        }
