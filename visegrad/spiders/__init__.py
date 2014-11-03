import scrapy
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher


class VisegradSpider(scrapy.Spider):
    exporter_class = None

    def __init__(self, *args, **kwargs):
        super(VisegradSpider, self).__init__(*args, **kwargs)

        dispatcher.connect(self.spider_opened, signals.spider_opened)

    def spider_opened(self, spider):
        pass
