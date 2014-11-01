import scrapy
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher


class VisegradSpider(scrapy.Spider):
    exporter_class = None

    def __init__(self, *args, **kwargs):
        super(VisegradSpider, self).__init__(*args, **kwargs)

        dispatcher.connect(self.spider_opened, signals.spider_opened)
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def spider_opened(self, spider):
        pass

    def spider_closed(self, spider, reason):
        if reason == 'finished' and self.exporter_class:
            exporter = self.exporter_class()
            exporter.run_export()
