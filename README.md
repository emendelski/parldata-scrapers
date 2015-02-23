# Installation
You need Python 2.7 and install system requirements.
```
sudo apt-get install python-dev libxml2-dev libxslt-dev libssl-dev libffi-dev
pip install -r requirements.txt
```

# Usage
Credentials are stored in JSON file private.json in project directory.

All Scrapy commands and settings (e.g. logging or throtlling) can be applied to these scrapers. Docs are [here](http://doc.scrapy.org/en/0.24/).

### Hungary
private.json
```
{
	"VPAPI_PWD_HU_ORSZAGGYULES": "secret",
	"HU_ORSZAGGYULES_ACCESS_TOKEN": "secret"
}
```
Command
```
scrapy crawl parlament.hu
```


### Montenegro
private.json
```
{
	"VPAPI_PWD_ME_SKUPSTINA": "secret"
}
```
Command
```
scrapy crawl skupstina.me
```

### Poland
private.json
```
{
	"VPAPI_PWD_PL_SEJM": "secret"
}
```
Command
```
scrapy crawl mojepanstwo.pl
```
