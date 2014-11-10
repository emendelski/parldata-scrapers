# Installation

```
pip install -r requirements.txt
```

# Usage

All Scrapy commands and settings (e.g. logging or throtlling) can be applied to these scrapers. Docs are [here](http://doc.scrapy.org/en/0.24/).

### Hungary
```
VPAPI_PARLIAMENT_ENDPOINT=hu/orszaggyules VPAPI_PWD_HU_ORSZAGGYULES=secret HU_ORSZAGGYULES_ACCESS_TOKEN=secret scrapy crawl parlament.hu
```

### Montenegro
```
VPAPI_PARLIAMENT_ENDPOINT=me/skupstina VPAPI_PWD_ME_SKUPSTINA=secret scrapy crawl skupstina.me
```

### Poland
```
VPAPI_PARLIAMENT_ENDPOINT=pl/sejm VPAPI_PWD_PL_SEJM=secret scrapy crawl mojepanstwo.pl
```
