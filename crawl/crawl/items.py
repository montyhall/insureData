# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class CrawlItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class YPAgent(scrapy.Item):
    # define the fields for your item here like:
    yp_url          = scrapy.Field()
    status          = scrapy.Field()
    crawlDate       = scrapy.Field()
    lastmodified    = scrapy.Field()
    name            = scrapy.Field()
    aka             = scrapy.Field()
    address         = scrapy.Field()
    phone           = scrapy.Field()
    email           = scrapy.Field()
    star_rating     = scrapy.Field()
    rating_count    = scrapy.Field()
    yp_preferred    = scrapy.Field()
    accreditation   = scrapy.Field()
    associations    = scrapy.Field()
    yrs_in_biz      = scrapy.Field()
    quote_page      = scrapy.Field()
    visit_page      = scrapy.Field()
    general_info    = scrapy.Field()
    services        = scrapy.Field()
    brands          = scrapy.Field()
    payments_accepted = scrapy.Field()
    neighborhoods    = scrapy.Field()
    langs           = scrapy.Field()
    otherlinks      = scrapy.Field()
    fb_link         = scrapy.Field()
    cats            = scrapy.Field()
    goodzer_url     = scrapy.Field()
    serviceDescription    = scrapy.Field()
    other_info      = scrapy.Field()
    reviews         = scrapy.Field()
    gallary_urls    = scrapy.Field()
