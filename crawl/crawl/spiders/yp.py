import re
import scrapy
from scrapy import signals
from scrapy.spiders import CrawlSpider
import json
import os
import pandas as pd
import urllib.parse
from crawl.items import YPAgent
from datetime import datetime
from dateutil.parser import parse as timeParser

'''
scrape YP

https://www.yellowpages.com/

scrapy crawl yp_insurance \
-a seedsFile='seeds/seeds.json' \
-a searchTerm=insurance \
-a statsFile=stats2.json \
-a failedFile=failed.txt \
-o data2.json

'''
class CrawlerSpider(CrawlSpider):
    name = "yp_insurance"

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

    def __init__(self,seedsFile: str='seeds/seeds.json',
                 searchTerm: str='insurance',
                 statsFile: str='stats.json',
                 failedFile: str='failed.txt') -> None:

        self.statsDir = 'stats'
        statsFile = os.path.join(self.statsDir, statsFile)
        os.makedirs(os.path.dirname(statsFile), exist_ok=True)

        failedFile = os.path.join(self.statsDir, failedFile)
        os.makedirs(os.path.dirname(failedFile), exist_ok=True)

        self.statsFile = statsFile
        self.failedurls = failedFile

        self.ypBase = 'https://www.yellowpages.com/'
        self.query = searchTerm

        #self.locations = self.readSeeds(seedsFile)

        self.locations = ['new-york-ny']

        self.failed_urls = []

        super(CrawlerSpider, self).__init__()

    def readSeeds(self,seedsFile: str) -> pd.DataFrame:
        return pd.read_json(seedsFile,orient='index').transpose()

    def spider_opened(self,spider):
        """ Handler for spider_closed signal. see:
        https://doc.scrapy.org/en/latest/topics/signals.html
        """
        spider.logger.info('Spider opened: %s', spider.name)

    def spider_closed(self,spider):
        """ Handler for spider_closed signal. see:
        https://doc.scrapy.org/en/latest/topics/signals.html
        pandas.read_json('injuries.json')
        """
        self.crawler.stats.set_value('failed_urls', len(self.failed_urls))

        with open(self.statsFile, 'w') as fp:
            d=self.crawler.stats.get_stats()
            d['start_time']=d['start_time'].isoformat()
            d['finish_time']=d['finish_time'].isoformat()
            json.dump(d, fp)

        with open(self.failedurls, 'w') as fp:
            for u in self.failed_urls:
                fp.write(u+"\n")

        #self.errorFile.close()

        spider.logger.info('Spider closed: %s', spider.name)

    def start_requests(self,page: int=1) -> None:
        for location in self.locations:
            qterm = location+"/"+self.query+"?page={}".format(page)
            seed = urllib.parse.urljoin(self.ypBase, qterm)
            yield scrapy.Request(url=seed,callback=self.parse, meta={'location': location,'page':page})

    # def containsSubStrClass(self,className):
    #     return "//div[contains(concat(' ', normalize-space(@class), ' '), '{}')]".format(className)

    def get_detail_page_info(self,response):
        agent = YPAgent()

        agent['yp_url'] = response.url
        agent['status'] = response.status
        agent['crawlDate'] = datetime.now().isoformat()

        # contact
        try:
            agent['name'] = response.xpath("//div[@class='sales-info']/h1/text()").get()
        except:
            pass
        try:
            agent['aka'] = response.xpath("//dd[@class='aka']/p/text()").getall()
        except:
            pass
        try:
            agent['address'] = response.xpath("//div[@class='contact']/h2/text()").get()
        except:
            pass
        try:
            agent['phone'] = response.xpath("//div[@class='contact']/p[@class='phone']/text()").get()
        except:
            pass
        try:
            agent['email'] = response.xpath("//a[@class='email-business']/@href").get().split("mailto:")[1]
        except:
            pass
        #meta
        try:
            agent['star_rating'] = response.xpath("//div[contains(concat(' ', "
                                       "normalize-space(@class), ' '), "
                                       "'rating-star')]/@class").get().split('rating-stars')[1].strip()
        except:
            pass
        try:
            agent['rating_count'] = int(re.search(r'\d+',response.xpath("//a[@class='yp-ratings']/span/text()").get()).group(0))
        except:
            pass
        try:
            badge = response.xpath("//img[@class='preferred-badge']").get()
            if badge:
                agent['yp_preferred'] = True
            else:
                agent['yp_preferred'] = False
        except:
            pass
        try:
            agent['accreditation'] = response.xpath('//dd[@class="accreditation"]/p/text()').getall()
        except:
            pass
        try:
            associations = response.xpath('//dd[@class="associations"]/text()').getall()
            if not associations:
                associations = response.xpath('//dd[@class="associations"]/p/text()').getall()
            agent['associations'] = associations
        except:
            pass
        try:
            agent['yrs_in_biz'] = int(response.xpath("//div[@class='years-in-business']/div[@class='count']/div[@class='number']/text()").get())
        except:
            pass
        try:
            # first try the simple link
            quote_url = response.xpath("//a[text()='Get a Quote']/@href").get()
            if not quote_url:
                # try to see if the big button is present
                quote_url = response.xpath("//div[@id='cta-offer']/a[@class='cta-link']/@href").get()
            agent['quote_page'] = quote_url
        except:
            pass

        try:
            agent['visit_page'] = response.xpath("//a[contains(concat(' ', "
                           "normalize-space(@class), ' '), "
                           "'website-link')]/@href").get()
        except:
            pass

        #body
        try:
            agent['general_info'] = response.xpath("//dd[@class='general-info']/text()").get()
        except:
            pass
        try:
            data = response.xpath("//dt[text()='Services/Products']/following-sibling::dd[1]/text()").get()
            if len(data.strip()) < 1:
                data = response.xpath("//dt[text()='Services/Products']/following-sibling::dd/p/text()").getall()
            if len(data.strip()) < 1:
                data = response.xpath("//dt[text()='Services/Products']/following-sibling::dd//li/text()").getall()
            agent['products'] = data
        except:
            pass
        try:
            agent['brands'] = response.xpath("//dd[@class='brands']/text()").get()
        except:
            pass
        try:
            agent['payments_accepted'] = response.xpath("//dd[@class='payment']/text()").get()
        except:
            pass
        try:
            agent['neighborhoods'] = response.xpath("//dd[@class='neighborhoods']/span/a/text()").getall()
        except:
            pass
        try:
            agent['langs'] = response.xpath("//dd[@class='languages']/text()").get()
        except:
            pass
        try:
            agent['otherlinks'] = list(set(response.xpath("//a[@class='other-links']/text()").getall()))
        except:
            pass
        try:
            agent['fb_link'] = response.xpath("//a[@class='fb-link']/@href").get()
        except:
            pass
        try:
            agent['categories'] = list(set(response.xpath("//dd[@class='categories']/span/a/text()").getall()))
        except:
            pass
        #side
        try:
            data = response.xpath("//aside[@id='main-aside']/section[@class='menu-links']/a[@class='more-link']/@href").get()
            if data:
                data = urllib.parse.urljoin(self.ypBase,data)
            agent['goodzer_url'] = data
        except:
            pass
        try:
            servicesList = response.xpath("//aside[@id='main-aside']/section[@class='menu-links']/a[@class='category']/text()").getall()
            c = response.xpath("//aside[@id='main-aside']/section[@class='menu-links']/ul")
            cache = {}
            for i,cat in enumerate(c):
                data=cat.xpath(".//div[@class='title-price']/text()").extract()
                if data:
                    data = [d.strip() for d in data]
                    cache[servicesList[i]] = data
            agent['serviceDescription'] = cache
        except:
            pass
        try:
            agent['other_info'] = response.xpath('//dd[@class="other-information"]/p/text()').get()
        except:
            pass
        try:
            agent['gallary_urls'] = response.xpath('//section[@id="gallery"]//div[@class="collage"]//a/img/@src').getall()
        except:
            pass
        try:
            agent['reviews'] = response.xpath('//div[@id="reviews-container"]//div[@class="review-response"]//p/text()').getall()
        except:
            pass

        return agent

    def parse(self, response):
        if response.status == 404:
            self.crawler.stats.inc_value('failed_url_count')
            self.failed_urls.append(response.url)

        # get agent links on 1 result page
        yp_urls = response.xpath('//div[@class="search-results organic"]/div[@class="result"]//a[@class="business-name"]/@href').getall()

        if response.meta['page'] == 1:
            # get total number of results for top-level query
            numResults = int(response.xpath('//div[@class="pagination"]/p/text()').get())
            self.pages = int(numResults/len(yp_urls))

        for yp_url in yp_urls:
            yp_url = urllib.parse.urljoin(self.ypBase, yp_url)
            yield scrapy.Request(url=yp_url, callback=self.parseDetailPage)

        # check if there isa 'next' page
        # xpath return format: '/burlington-vt/insurance?page=2'
        nextPageURL = response.xpath('//a[@class="next ajax-page"]/@href').get()
        if nextPageURL:
            location = nextPageURL.split('/')[1]
            page = int(nextPageURL.split('page=')[1])
            seed = urllib.parse.urljoin(self.ypBase, nextPageURL)
            yield scrapy.Request(url=seed, callback=self.parse, meta={'location': location, 'page': page})


    def parseDetailPage(self,response):
        if response.status == 404:
            self.crawler.stats.inc_value('failed_url_count')
            self.failed_urls.append(response.url)
        #self.logger.info('parsing: {}'.format(response.url))
        return self.get_detail_page_info(response)

    def process_exception(self, response, exception, spider):
        ex_class = "%s.%s" % (exception.__class__.__module__, exception.__class__.__name__)
        self.crawler.stats.inc_value('downloader/exception_count', spider=spider)
        self.crawler.stats.inc_value('downloader/exception_type_count/%s' % ex_class, spider=spider)