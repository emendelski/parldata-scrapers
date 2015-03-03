import scrapy
from scrapy.conf import settings
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher

from datetime import datetime

import vpapi


class VisegradSpider(scrapy.Spider):
    exporter_class = None
    user = 'scraper'
    parliament_code = ''
    latest_dates = {}

    def __init__(self, *args, **kwargs):
        super(VisegradSpider, self).__init__(*args, **kwargs)

        vpapi.parliament(self.get_parliament())
        vpapi.authorize(self.get_user(), self.get_password())

        dispatcher.connect(self.spider_opened, signals.spider_opened)

    def spider_opened(self, spider):
        self.log_start()

    def log_start(self):
        log_item = {
            'status': 'running'
        }
        if settings.get('LOG_FILE'):
            log_item['file'] = settings['LOG_FILE']
        self._log = vpapi.post('logs', log_item)

    def log_finish(self, status):
        vpapi.patch('logs/%s' % self._log['id'], {'status': status})

    def get_parliament(self):
        default_endpoint = '/'.join(self.parliament_code.lower().split('_'))
        return settings.get('VPAPI_PARLIAMENT_ENDPOINT', default_endpoint)

    def get_user(self):
        return self.user

    def get_password(self):
        var = 'VPAPI_PWD_%s' % self.parliament_code.upper()
        return settings.get(var)

    def get_latest_item(self, endpoint, time_key):
        return vpapi.getfirst(endpoint, sort='-%s' % time_key)

    def get_latest_date(self, endpoint, time_key):
        if not settings.get('CRAWL_LATEST_ONLY'):
            return None

        if endpoint in self.latest_dates:
            return self.latest_dates[endpoint]

        latest = self.get_latest_item(endpoint, time_key)
        if latest and time_key in latest:
            dt = datetime.strptime(latest[time_key], '%Y-%m-%dT%H:%M:%S')
            self.latest_dates[endpoint] = dt.date()
            return dt.date()

        self.latest_dates[endpoint] = None

    def get_latest_vote_event_date(self):
        return self.get_latest_date('vote-events', 'start_date')

    def get_latest_speech_date(self):
        return self.get_latest_date('speeches', 'date')
