import scrapy
from scrapy.conf import settings
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher

import vpapi


class VisegradSpider(scrapy.Spider):
    exporter_class = None
    user = 'scraper'
    parliament_code = ''

    def __init__(self, *args, **kwargs):
        super(VisegradSpider, self).__init__(*args, **kwargs)

        vpapi.parliament(self.get_parliament())
        vpapi.authorize(self.get_user(), self.get_password())

        dispatcher.connect(self.spider_opened, signals.spider_opened)

    def spider_opened(self, spider):
        self.log_start()

    def log_start(self):
        self._log = vpapi.post(
            'logs',
            {
                'status': 'running'
            }
        )

    def log_finish(self, status):
        vpapi.patch('logs/%s' % self._log['id'], {'status': status})

    def get_parliament(self):
        return settings.get('VPAPI_PARLIAMENT_ENDPOINT')

    def get_user(self):
        return self.user

    def get_password(self):
        var = 'VPAPI_PWD_%s' % self.parliament_code.upper()
        return settings.get(var)
