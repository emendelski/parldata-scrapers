from scrapy.exceptions import DropItem
from scrapy.conf import settings
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from scrapy.contrib.exporter import JsonLinesItemExporter
from scrapy.log import ERROR

import os


class DuplicatesPipeline(object):
    def __init__(self):
        self.items_seen = set()

    def process_item(self, item, spider):
        if 'identifiers' in item:
            if type(item['identifiers']) is list:
                identifier = item['identifiers'][0]
                k = identifier['identifier']
                scheme = identifier.get('scheme')
                if scheme:
                    k = "%s/%s" % (scheme, k)
            else:
                k = item['identifiers']
            t = (type(item), k)

            if t in self.items_seen:
                raise DropItem()
            self.items_seen.add(t)
        return item


class ExportPipeline(object):
    def __init__(self):
        self.files = {}
        self.exporters = {}

        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def get_filename(self, spider, item):
        directory = spider.name
        item_file = item.__class__.__name__ + '.json'
        return os.path.join(settings.get('OUTPUT_PATH', ''), directory, item_file)

    def get_file(self, spider, item):
        filename = self.get_filename(spider, item)
        if filename not in self.files:
            dirs, f = os.path.split(filename)
            if not os.path.exists(dirs):
                os.makedirs(dirs)
            self.files[filename] = open(filename, "wb")
        return self.files[filename]

    def get_exporter(self, spider, item):
        filename = self.get_filename(spider, item)
        if filename not in self.exporters:
            f = self.get_file(spider, item)
            self.exporters[filename] = JsonLinesItemExporter(f)
            self.exporters[filename].start_exporting()
        return self.exporters[filename]

    def spider_closed(self, spider, reason):
        for filename in self.files:
            if filename in self.exporters:
                self.exporters[filename].finish_exporting()
            self.files[filename].close()

        if reason == 'finished' and spider.exporter_class:
            exporter = spider.exporter_class(log=spider.log)
            try:
                exporter.run_export()
            except Exception, e:
                spider.log(e.message, ERROR)

    def process_item(self, item, spider):
        self.get_exporter(spider, item).export_item(item)

        return item
