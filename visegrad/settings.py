# Scrapy settings for visegrad project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#

BOT_NAME = 'visegrad'

SPIDER_MODULES = ['visegrad.spiders']
NEWSPIDER_MODULE = 'visegrad.spiders'

ITEM_PIPELINES = {
    'visegrad.pipelines.DuplicatesPipeline': 800,
    'visegrad.pipelines.ExportPipeline': 900,
}

OUTPUT_PATH = 'data'

# DOWNLOAD_DELAY = 0.90
# RANDOMIZE_DOWNLOAD_DELAY = False

# Crawl responsibly by identifying yourself (and your website) on the user-agent
# USER_AGENT = 'visegrad (+http://www.yourdomain.com)'

try:
    import json
    import os.path
    from datetime import datetime
    with open('private.json', 'r') as f:
        private = json.loads(f.read())
        for key in private:
            globals()[key] = private[key]
        if 'VPAPI_PARLIAMENT_ENDPOINT' in private:
            now = datetime.now
            filename = "scrapy-%s.log" % now().strftime("%Y-%m-%dT%H:%M:%S")
            global LOG_FILE
            LOG_FILE = os.path.join(
                '/var/log/scrapers/',
                private['VPAPI_PARLIAMENT_ENDPOINT'],
                filename
            )
except IOError:
    pass
