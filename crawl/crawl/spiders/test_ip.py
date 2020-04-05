import scrapy
from time import sleep

class TestSpider(scrapy.Spider):
    name = 'test_ip'
    custom_settings = {
        'CONCURRENT_REQUESTS': 1,
        'DOWNLOAD_DELAY': 0,
        # 'DOWNLOADER_MIDDLEWARES': {
           # 'tutorial.middlewares.middlewares.TutorialDownloaderMiddleware': 543,
           # 'tutorial.middlewares.Tor.TorMiddleware': 100,   
        # }
    }
    def start_requests(self):
        url = 'http://checkip.amazonaws.com'
        i=0
        while True:
            i+=1
            print('-'*10)
            print(i)
            yield scrapy.Request(url, callback=self.parse)
            sleep(0.5)

    def parse(self, response):
        print('ip is: {}'.format(response.text))
        yield {
            'your ip': response.text
        }