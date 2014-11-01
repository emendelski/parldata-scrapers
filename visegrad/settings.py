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
