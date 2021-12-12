import scrapy
import sqlalchemy
import pandas as pd
import os
import json
from scrapy.crawler import CrawlerProcess

# %%
# load database

engine = sqlalchemy.create_engine('postgresql://{0}:{1}@{2}:{3}/trading'.format(os.environ.get('db_user'), os.environ.get('db_password'),
                                                                                os.environ.get('db_host'), os.environ.get('db_port')))

ticker_list = list(pd.read_sql_query('''SELECT ticker FROM portfolio
                                        WHERE type_id=1 ORDER BY ticker''', engine)['ticker'])

# %%
# list of url
stock_url = []

for ticker in ticker_list:
    url = 'https://frames.pse.com.ph/security/{0}'.format(ticker)
    stock_url.append(url)

# %%


class PSE_stock_spider(scrapy.Spider):
    name = 'PSE_stockdata'
    allowed_domains = stock_url
    start_urls = stock_url

    def start_requests(self):
        urls = stock_url
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        for row in response.xpath('//*[@class="table table-borderless table-hover border table-resizable sortable"]//tbody//tr'):
            yield {
                'date': row.xpath('td[1]//text()').get().replace(',', ''),
                'open': row.xpath('td[2]//text()').get().replace(',', ''),
                'high': row.xpath('td[3]//text()').get().replace(',', ''),
                'low': row.xpath('td[4]//text()').get().replace(',', ''),
                'close': row.xpath('td[5]//text()').get().replace(',', ''),
                'volume': row.xpath('td[7]//text()').get().replace(',', ''),
                'ticker': response.selector.xpath('//*[@id="security_dynamic"]/div/div[2]/div/div[1]/div[1]/div[1]/div[1]/h3/text()').get(),
            }

# %%


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(PSE_stock_spider)
    process.start()


# %%
