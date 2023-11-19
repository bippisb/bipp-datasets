import scrapy


class devanagiri_spider(scrapy.Spider):
    name = "devanagiri"

    start_urls = ["https://jrgraphix.net/r/Unicode/0900-097F"]

    def parse(self, response):
        self.logger.info("Scrapper for Devangiri - Unicode dictionary")
        yield {
            "devan_text": response.css("span::text").getall(),
            "unicode": response.css("tt::text").getall(),
        }
