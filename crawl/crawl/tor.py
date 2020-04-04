from scrapy import signals
import os
from scrapy import settings


# Send "Change IP" signal to tor control port 
class TorMiddleware(object):
    
    def __init__(self,crawler):
        """
        Initializes database connection and sessionmaker.
        Creates deals table.
        """
        self.crawler = crawler
        self.settings    = crawler.settings

    def process_request(self, request, spider):
        """
        You must first install the nc program and Tor service on your GNU Linux operating system
        After that and change /etc/tor/torrc, add
        control port and password to it.
        install privoxy for having HTTP and HTTPS over torSOCKS5
		"""
        #Deploy : add controlport and password to /etc/tor/torrc
        os.system("""(echo authenticate "vale2424"; echo signal newnym; echo quit) | nc localhost 9050""")
        request.meta['proxy'] = self.settings.get('HTTP_PROXY')