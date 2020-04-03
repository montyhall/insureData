import scrapy
from scrapy import signals
from scrapy.spiders import CrawlSpider
import json
from lxml import html
import requests
import urllib.request
import urllib.parse
import os
'''
scrape YP

https://www.yellowpages.com/search?search_terms=insurance&geo_location_terms=New%20York%2C%20NY

scrapy crawl yp_locations -a statsFile=cities_stats.csv -a seedsFile=seeds.json

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

    def __init__(self,statsFile: str='cities_stats.csv',seedsFile: str='seeds.json') -> None:

        self.statsFile = statsFile
        self.ypBase = 'https://www.yellowpages.com/'
        self.cities,self.states = None,None

        # get sitemap
        self.getCitiesAndStatesFromSiteMapPage()
        self.seeds={}

        self.dataDir = 'seeds'
        self.seedsFile = os.path.join(self.dataDir, seedsFile)
        os.makedirs(os.path.dirname(self.seedsFile), exist_ok=True)

        super(CrawlerSpider, self).__init__()

    def getCitiesAndStatesFromSiteMapPage(self):
        '''
        get cities and states from https://www.yellowpages.com/sitemap
        :return:
        '''
        pageContent = requests.get(urllib.parse.urljoin(self.ypBase, "sitemap"))
        tree = html.fromstring(pageContent.content)
        self.cities = self.get_popular_cities(tree)
        self.states = self.get_popular_states(tree)

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
        with open(self.seedsFile, 'w') as fp:
            json.dump(self.seeds, fp)

        with open(self.statsFile, 'w') as fp:
            d=self.crawler.stats.get_stats()
            d['start_time']=d['start_time'].isoformat()
            d['finish_time']=d['finish_time'].isoformat()
            json.dump(d, fp)

        spider.logger.info('Spider closed: %s', spider.name)


    def start_requests(self) -> None:
        for seed in self.states:
            yield scrapy.Request(url=seed,callback=self.parse, meta={'state': seed})

    def get_popular_cities(self,tree):
        # houston-tx
        return tree.xpath('//p[@id="popularcities"]/following-sibling::section/ul/li/a/@href')

    def get_popular_states(self, tree):
        states = tree.xpath('//h2[text()="Local Yellow Pages"]/following-sibling::section/ul/li/a/@href')
        return [urllib.parse.urljoin(self.ypBase, state) for state in states]

    def parse(self, response):
        state = response.meta['state']
        urls = response.xpath('//div[@class="row"][1]/section/ul/li/a/@href').getall()
        self.seeds[state] = [urllib.parse.urljoin(self.ypBase, state) for state in urls]

