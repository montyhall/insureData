import re
import scrapy
from scrapy import signals
from scrapy.spiders import CrawlSpider
import json
import os
import urllib.parse
from crawl.items import YPAgent
from datetime import datetime
from dateutil.parser import parse as timeParser

'''
scrape YP

https://www.yellowpages.com/search?search_terms=insurance&geo_location_terms=New%20York%2C%20NY

scrapy crawl yp_insurance -a searchTerm=insurance -a statsFile=stats.csv -o data.json

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

    def __init__(self,searchTerm: str='insurance',statsFile: str='stats.csv') -> None:

        self.statsFile = statsFile

        self.ypBase = 'https://www.yellowpages.com/'
        self.searchBaseURL = urllib.parse.urljoin(self.ypBase,'search?search_terms={}'.format(searchTerm))

        self.locations = ['"New York, NY"']

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


    def start_requests(self):
        for location in self.locations:
            #url_i = urllib.parse.quote_plus(self.searchBaseURL+'&geo_location_terms={}'.format(location))
            seed = self.searchBaseURL+'&geo_location_terms={}'.format(location)
            print('\n\n\n')
            print('parsing: {}'.format(seed))
            yield scrapy.Request(url=seed,callback=self.parse, meta={'location': location,'page':0,'depth': 0})

    def get_detail_page_UR2L(self,response):
        print('\n\n\n')
        print('-'*100)
        print('getting urls')
        self.logger.info('getting urls')
        print(response.request.meta)
        yp_urls = response.xpath("//a[@class='business-name']/@href").extract()
        for yp_url in yp_urls:
            yp_url = urllib.parse.urljoin(self.ypBase,yp_url)
            print('\n\n\n')
            print(yp_url)

    def get_detail_page_URL(self,response):
        print('\n\n\n')
        print('-'*100)
        print('getting urls')
        self.logger.info('getting urls')
        # YP url
        yp_urls = response.xpath("//a[@class='business-name']/@href").extract()
        for yp_url in yp_urls:
            yp_url = urllib.parse.urljoin(self.ypBase,yp_url)
            yield scrapy.Request(url=yp_url, callback=self.parseDetailPage, meta={'depth': 1})

    def containsSubStrClass(self,className):
        return "//div[contains(concat(' ', normalize-space(@class), ' '), '{}')]".format(className)

    def get_detail_page_info(self,response):

        lastmodified,depth= None,None

        if 'Last-Modified' in response.headers and response.headers['Last-Modified']:
            lastmodified = timeParser(response.headers['Last-Modified']).isoformat()

        agent = YPAgent()

        agent['yp_url'] = response.url
        agent['status'] = response.status
        agent['crawlDate'] = datetime.now().isoformat()
        agent['lastmodified'] = lastmodified

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
            agent['accreditation'] = response.xpath('//dd[@class="accreditation"]/p/text()').get()
        except:
            pass
        try:
            associations = response.xpath('//dd[@class="associations"]/text()').get()
            if not associations:
                associations = response.xpath('//dd[@class="associations"]/p/text()').get()
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
            agent['services'] = data
        except:
            pass
        # try:
        #     brand = response.xpath("//dt[@class='Services/Products']/text()").get()
        #     if not brand:
        #         brand = response.xpath("//dd[@class='brands']/text()").get()
        #     agent['brands'] = brand
        # except:
        #     pass
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
            agent['cats'] = list(set(response.xpath("//dd[@class='categories']/span/a/text()").getall()))
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

        print('\n\n\n')
        print(agent)
        print('\n\n\n')

        return agent

    def parse(self, response):
        # get total number of results for top-level query
        numResults = int(response.xpath('//div[@class="pagination"]/p/text()').get())

        # get agent links on 1 result page
        yp_urls = response.xpath("//a[@class='business-name']/@href").extract()

        for yp_url in yp_urls:
            yp_url = urllib.parse.urljoin(self.ypBase, yp_url)
            yield scrapy.Request(url=yp_url, callback=self.parseDetailPage)

    def parseDetailPage(self,response):
        #self.logger.info('parsing: {}'.format(response.url))
        return self.get_detail_page_info(response)
