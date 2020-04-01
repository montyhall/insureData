from scrapy.xlib.pydispatch import dispatcher
import re
import scrapy
from scrapy import signals
from scrapy.spiders import CrawlSpider
import json
import os
import urllib.parse

'''
scrape YP

https://www.yellowpages.com/search?search_terms=insurance&geo_location_terms=New%20York%2C%20NY

scrapy crawl yp_insurance -a filename=ff.json
'''
class CrawlerSpider(CrawlSpider):
    name = "yp_insurance"

    def __init__(self,
                 searchTerm: str='insurance',
                 filename: str='transactions.json') -> None:

        dispatcher.connect(self.spider_closed, signals.spider_closed)

        self.ypBase = 'https://www.yellowpages.com/'
        self.searchBaseURL = urllib.parse.urljoin(self.ypBase,'search?search_terms={}'.format(searchTerm))

        self.locations = ['New York\, NY']

        self.dataDir = 'data/transactions'

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
        print('writing ({}) rows of data to: {}'.format(len(self.data),self.transFile))
        with open(self.transFile, 'w') as fp:
            json.dump(self.data, fp)


        spider.logger.info('Spider closed: %s', spider.name)

    def start_requests(self):
        for location in self.locations:
            url = urllib.parse.urljoin(self.searchBaseURL+'&geo_location_terms='.format(location))
            yield scrapy.Request(url=url,callback=self.parse, meta={'location': location,'page':0,'depth': 0})

    def get_detail_page_URL(self,response):

        # YP url
        yp_urls = response.xpath("//a[@class='business-name']/@href").extract()
        for yp_url in yp_urls:
            yp_url = urllib.parse.urljoin(self.ypBase,yp_url)
            yield scrapy.Request(url=yp_url, callback=self.parse, meta={'depth': 1})


    def containsSubStrClass(className):
        return "//div[contains(concat(' ', normalize-space(@class), ' '), '{}')]".format(className)

    def get_detail_page_info(self,response):

        # contact
        biz_name = response.xpath("//div[@class='sales-info']/h1/text()").get()
        biz_address = response.xpath("//div[@class='contact']/h2/text()").get()
        biz_phone = int(response.xpath("//div[@class='contact']/p[@class='phone']/text()").get())
        biz_email = response.xpath("//a[@class='email-business']/@href").get().split("mailto:")[1]

        #meta
        ratings_stars = response.xpath("//div[contains(concat(' ', "
                                       "normalize-space(@class), ' '), "
                                       "'rating-star')]/@class").get().split('rating-stars')[1].strip()

        ratings_counts = int(re.search(r'\d+',response.xpath("//a[@class='yp-ratings']/span/text()").get()).group(0))
        yrs_in_biz = int(response.xpath("//div[@class='years-in-business']/div[@class='count']/div[@class='number']/text()").get())

        #body
        general_info = response.xpath("//dd[@class='general-info']/text()").get()
        services = response.xpath("//dt[text()='Services/Products']/following-sibling::dd[1]/text()").get()
        brands = response.xpath("//dt[@class='Services/Products']/text()").get()
        payments = response.xpath("//dd[@class='payment']/text()").get()
        neighborhoods = response.xpath("//dd[@class='neighborhoods']/span/a/text()").getall()
        langs = response.xpath("//dd[@class='languages']/text()").getall()
        otherlinks = set(response.xpath("//a[@class='other-links']/text()").getall())
        fb_link = response.xpath("//a[@class='fb-link']/@href").get()
        cats = set(response.xpath("//dd[@class='categories']/span/a/text()").getall())

        #side
        goodzer_url = urllib.parse.urljoin(self.ypBase,
                                           response.xpath("//aside[@id='main-aside']/section[@class='menu-links']/a[@class='more-link']/@href").get())
        servicesList = response.xpath("//aside[@id='main-aside']/section[@class='menu-links']/a[@class='category']/text()").getall()
        c = response.xpath("//aside[@id='main-aside']/section[@class='menu-links']/ul")
        cache = {}
        for i,cat in enumerate(c):
            data=cat.xpath(".//div[@class='title-price']/text()").extract()
            data = [d.strip() for d in data]
            cache[servicesList[i]] = data


    def parse(self, response):
        if response.request.meta['depth'] == 0:
            self.get_detail_page_URL(response)
        else:
            self.get_detail_page_info(response)


    # process_req: sits between request that was just made and before it is being downloaded.
    def process_req(self, req):
        pass
        # '''
        # This method act as sort of middleware between the
        # time the link is extracted and processed/downloaded.
        # :param req:
        # :return:
        # '''
        # req.meta['seed']=False
        # return req.replace(meta=req.meta)

