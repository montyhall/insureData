import re
import scrapy
from scrapy import signals
from scrapy.spiders import CrawlSpider
import json
import os
from lxml import html
import requests
import urllib.request
import urllib.parse
from crawl.items import YPAgent
from datetime import datetime
from dateutil.parser import parse as timeParser

'''
scrape YP

https://www.yellowpages.com/search?search_terms=insurance&geo_location_terms=New%20York%2C%20NY

scrapy crawl yp_locations -a statsFile=cities_stats.csv -o cities_data.json

scrapy crawl yp_insurance \
-a seedfile=data.csv \
-a statsFile=stats.csv \
-a errorFile=errors.csv \
-s DEPTH_LIMIT=2 \
-s es=True \
-s s3=False \
-o data.json \
-t json \
--logfile logs/log.txt

'''
class CrawlerSpider(CrawlSpider):
    name = "yp_locations"

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        '''
        from_crawler is often used for getting a reference to the crawler object
        (that holds references to objects like settings, stats, etc) and then
        either pass it as arguments to the object being created or set attributes to it.
        :param crawler:
        :param args:
        :param kwargs:
        :return:
        '''
        spider = super(CrawlerSpider, cls).from_crawler(crawler, *args, **kwargs)
        #crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def __init__(self,statsFile: str='stats.csv') -> None:

        self.statsFile = statsFile

        self.ypBase = 'https://www.yellowpages.com/'

        pageContent = requests.get(urllib.parse.urljoin(self.ypBase, "sitemap"))
        tree = html.fromstring(pageContent.content)
        self.cities = self.get_popular_cities(tree)
        self.states = self.get_popular_states(tree)

        #self.dataDir = 'data/transactions'

        # self.transFile = os.path.join(self.dataDir, filename)
        # os.makedirs(os.path.dirname(self.transFile), exist_ok=True)

        super(CrawlerSpider, self).__init__()

    def spider_opened2(self,spider):
        """ Handler for spider_closed signal. see:
        https://doc.scrapy.org/en/latest/topics/signals.html
        """
        spider.logger.info('Spider opened: %s', spider.name)

    def spider_closed(self,spider):
        """ Handler for spider_closed signal. see:
        https://doc.scrapy.org/en/latest/topics/signals.html
        pandas.read_json('injuries.json')
        """
        # print('writing ({}) rows of data to: {}'.format(len(self.data),self.transFile))
        # with open(self.transFile, 'w') as fp:
        #     json.dump(self.data, fp)

        with open(self.statsFile, 'w') as fp:
            d=self.crawler.stats.get_stats()
            d['start_time']=d['start_time'].isoformat()
            d['finish_time']=d['finish_time'].isoformat()
            json.dump(d, fp)

        #self.errorFile.close()

        spider.logger.info('Spider closed: %s', spider.name)


    def start_requests(self) -> None:
        for seed in self.states:
            yield scrapy.Request(url=seed,callback=self.parse, meta={'state': seed})

    def containsSubStrClass(self,className):
        return "//div[contains(concat(' ', normalize-space(@class), ' '), '{}')]".format(className)

    def get_popular_cities(self,tree):
        # houston-tx
        return tree.xpath('//p[@id="popularcities"]/following-sibling::section/ul/li/a/@href')

    def get_popular_states(self, tree):
        states = tree.xpath('//h2[text()="Local Yellow Pages"]/following-sibling::section/ul/li/a/@href')
        return [urllib.parse.urljoin(self.ypBase, state) for state in states]

    def get_popular_cities_in_state(self, tree):
        states = tree.xpath('//h2[text()="Local Yellow Pages"]/following-sibling::section/ul/li/a/@href')
        return [urllib.parse.urljoin(self.ypBase, state) for state in states]


    def parse(self, response):
        # get agent links on 1 result page
        yp_urls = response.xpath('//div[@class="search-results organic"]/div[@class="result"]//a[@class="business-name"]/@href').getall()

        if response.meta['page'] == 1:
            # get total number of results for top-level query
            numResults = int(response.xpath('//div[@class="pagination"]/p/text()').get())
            self.pages = int(numResults/len(yp_urls))

        for yp_url in yp_urls:
            # print('-'*100)
            # print(yp_url)
            # print(self.ypBase)
            yp_url = urllib.parse.urljoin(self.ypBase, yp_url)
            yield scrapy.Request(url=yp_url, callback=self.parseDetailPage)

        #response.xpath('//a[@class="next ajax-page"]/@href').get()

    def parseDetailPage(self,response):
        #self.logger.info('parsing: {}'.format(response.url))
        return self.get_detail_page_info(response)
