from scrapy import signals
import os
from scrapy.utils.project import get_project_settings
from stem import Signal
from stem.control import Controller
from toripchanger import TorIpChanger

from stem.util.log import get_logger

# logger = get_logger()
# logger.propagate = False

# Default settings.
REUSE_THRESHOLD = 1
LOCAL_HTTP_PROXY = "127.0.0.1:8118"
NEW_IP_MAX_ATTEMPTS = 10
TOR_PASSWORD = "vale2424"
TOR_ADDRESS = "127.0.0.1"
TOR_PORT = 9051
POST_NEW_IP_SLEEP = 0.5

ip_changer = TorIpChanger(reuse_threshold=REUSE_THRESHOLD,
                          tor_password=TOR_PASSWORD,
                          tor_port=TOR_PORT,
                          local_http_proxy=LOCAL_HTTP_PROXY)

# Send "Change IP" signal to tor control port 
class TorMiddleware(object):
    
    def __init__(self):
        self.settings = get_project_settings()
        self._requests_count = 0
        self.controller = Controller.from_port(address=TOR_ADDRESS,
                                               port=TOR_PORT)
        self.controller.authenticate(password=TOR_PASSWORD)
    
    def set_new_ip(self):
        return TorIpChanger(reuse_threshold=0,
                            tor_password='"vale2424"', 
                            tor_port=9051, 
                            local_http_proxy=self.settings.get('HTTP_PROXY')).get_new_ip()
    
    def set_new_ip2(self):
        """Change IP using TOR
        netstat -tulnp | grep 9051
        """
        with Controller.from_port(port=TOR_ADDRESS) as controller:
            controller.authenticate(password=TOR_PASSWORD)
            controller.signal(Signal.NEWNYM)
    
    def set_new_ip3(self):
        self.controller.signal(Signal.NEWNYM)
        
    def process_request(self, request, spider):
        """
        You must first install the nc program and Tor service on your GNU Linux operating system
        After that and change /etc/tor/torrc, add
        control port and password to it.
        install privoxy for having HTTP and HTTPS over torSOCKS5
		"""
        #Deploy : add controlport and password to /etc/tor/torrc
        #os.system("""(echo authenticate '"vale2424"'; echo signal newnym; echo quit) | nc localhost 9051""")
        self._requests_count += 1
        if self._requests_count > 5:
            self._requests_count = 0 
            #self.set_new_ip()
            #ip_changer.get_new_ip()
            self.set_new_ip3()
            
        request.meta['proxy'] = self.settings.get('HTTP_PROXY')
        spider.log('Proxy : %s' % request.meta['proxy'])
        
        # install tor,privoxy and TorIpChanger https://gist.github.com/DusanMadar/8d11026b7ce0bce6a67f7dd87b999f6b
        # https://github.com/DusanMadar/TorIpChanger
        