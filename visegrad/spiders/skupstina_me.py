# -*- coding: utf-8 -*-
import scrapy

import re

from urlparse import urljoin, parse_qs

from visegrad.spiders import VisegradSpider
from visegrad.items import Person, Organization, Membership, Motion, Event,\
        Speech
from visegrad.loaders import SkupstinaMePersonLoader, OrganizationLoader, MembershipLoader, \
        SkupstinaMeMotionLoader, SkupstinaMeEventLoader,\
        SkupstinaMeSpeechLoader
from visegrad.api.parliaments import SkustinaMeApiExport


def get_person_id(url):
    query = parse_qs(url)
    if 'id' in query:
        return query['id'][0]
    regex = re.compile(r'/(?P<id>\d+)[\w\-]+$')
    search = regex.search(url)
    if search:
        return search.group('id')


class SkupstinaMeSpider(VisegradSpider):
    name = "skupstina.me"
    allowed_domains = ["www.skupstina.me"]
    start_urls = (
        'http://www.skupstina.me/',
    )
    parliament_code = 'ME_SKUPSTINA'
    exporter_class = SkustinaMeApiExport

    MP_LIST_URL = 'http://www.skupstina.me/index.php/me/skupstina\
/poslanice-i-poslanici/lista-poslanika-i-poslanica'
    COMMITEES_LIST_URL = 'http://www.skupstina.me/index.php/me/\
administrativni-odbor/aktuelnosti'
    MOTIONS_LIST_URL = 'http://www.skupstina.me/~skupcg/skupstina/\
index.php?strana=zakoni&search=true'
    SITTINGS_LIST_URL = 'http://www.skupstina.me/~skupcg/skupstina/\
index.php?strana=sjednice&tipS=0'
    PARLIAMENT_ID = '17'

    def make_requests_from_iterable(self, urls, base_url = None, **kwargs):
        for url in urls:
            if base_url:
                url = urljoin(base_url, url)
            yield scrapy.Request(url, **kwargs)

    def start_requests(self):
        yield scrapy.Request(self.MP_LIST_URL, callback=self.parse_people)
        yield scrapy.Request(
            self.COMMITEES_LIST_URL, callback=self.parse_commitee_list)
        yield scrapy.FormRequest(
            self.MOTIONS_LIST_URL,
            formdata={'saziv': self.PARLIAMENT_ID},
            method='POST',
            callback=self.parse_motions
        )
        yield scrapy.Request(
            self.SITTINGS_LIST_URL, callback=self.parse_sittings)

    def parse_people(self, response):
        links = response.css('.poslanici h3 a::attr(href)').extract()

        return self.make_requests_from_iterable(links, base_url=response.url,
            callback=self.parse_person)

    def parse_person(self, response):
        content = response.css('.item-page')
        breadcrumbs = response.css('ul.breadcrumb a.pathway')

        l = SkupstinaMePersonLoader(item=Person(), selector=content,
            scheme='skupstina.me/people')
        person_id = get_person_id(response.url)
        l.add_value('identifiers', person_id)
        l.add_css('name', '.page-header h2 a::text')
        xp = u".//h3[contains(text(), 'Lični podaci')]//following-sibling::p[1]"
        l.add_xpath('birth_date', xp, re=r'\d{2}\.\d{2}\.\d{4}')
        l.add_css('image', 'img::attr(src)')
        l.add_value('sources', [response.url])
        person = l.load_item()
        yield person

        party_node = breadcrumbs[-1]
        party_id = party_node.css('::attr(href)').re(r'/([\w\-]+)$')
        party_name = party_node.css('::text').extract()

        p = OrganizationLoader(
            item=Organization(classification='party'),
            scheme='skupstina.me/parties'
        )
        p.add_value('name', party_name)
        p.add_value('identifiers', party_id)
        party = p.load_item()
        yield party

        m = MembershipLoader(item=Membership())
        m.add_value('person_id', person['identifiers'])
        m.add_value('organization_id', party['identifiers'])
        yield m.load_item()


    def parse_commitee_list(self, response):
        menu = response.css('#aside')
        links = menu.css('ul.nav li.parent > a::attr(href)').extract()
        # open "members" pages
        links = map(lambda x: x.replace('/aktuelnosti', '/sastav'), links)
        return self.make_requests_from_iterable(links, base_url=response.url,
            callback=self.parse_commitee)

    def parse_commitee(self, response):
        menu = response.css('#aside')
        content = response.css('#content')

        committee_id = menu.css('.active.parent > a::attr(href)').re(
            r'/index\.php/me/(?P<extract>[\w\-]+)/')[0]

        l = OrganizationLoader(
            item=Organization(classification='committee'),
            selector=menu,
            scheme='skupstina.me/committees'
        )
        l.add_css('name', '.active.parent > a::text')
        l.add_value('identifiers', committee_id)
        l.add_value('sources', [response.url.replace('/sastav', '/aktuelnosti')])
        committee = l.load_item()
        yield committee

        members = content.css('h3 a::attr(href)').extract()

        for member in members:
            pk = get_person_id(member)
            l = MembershipLoader(item=Membership())
            l.add_value('person_id', {
                'scheme': 'skupstina.me/people', 'identifier': pk
            })
            l.add_value('organization_id', committee['identifiers'])
            l.add_value('sources', [response.url])
            yield l.load_item()

        reqs = self.make_requests_from_iterable(members, base_url=response.url,
            callback=self.parse_person)
        for req in reqs:
            yield req

    def parse_motions(self, response):
        content = response.css('#PretragaZakona')

        motions = []
        motion_dict = {}

        # very slow and cpu intensive query
        trs = content.xpath(".//tr[td[contains(@class,'sjednica')]] | \
.//tr[td[contains(@class,'poslanici')]]")

        for tr in trs:
            name = tr.css('.poslanici font::text').extract()
            info = tr.css('.sjednica font::text')
            if name:
                # header found, start new item
                if motion_dict:
                    motions.append(motion_dict)
                path = tr.css('a::attr(href)').extract()[0]
                url = urljoin('http://www.skupstina.me/', path)
                motion_dict = {
                    'name': name[0],
                    'sources': [url]
                }
            elif info:
                key = ''.join(info[0].re(r'([\w\ ]+):')).lower()
                if key:
                    value = info[1].extract()
                    motion_dict[key] = value
        if motion_dict:
            motions.append(motion_dict)

        keys = {
            'status': 'result',
            'datum': 'date'
        }

        for motion_dict in motions:
            l = SkupstinaMeMotionLoader(item=Motion())
            l.add_value('text', motion_dict['name'])
            l.add_value('sources', motion_dict['sources'])
            for k in keys:
                value = motion_dict.get(k)
                if value:
                    l.add_value(keys[k], value)
            yield l.load_item()

    def parse_sittings(self, response):
        links = response.css('td.sjednica a')
        for link in links:
            url = urljoin(response.url, link.xpath('.//@href').extract()[0])
            name = link.xpath('.//text()').extract()
            yield scrapy.Request(
                url,
                callback=self.parse_sitting,
                meta={
                    'name': name
                }
            )

    def parse_sitting(self, response):
        content = response.css('.center_content')
        l = SkupstinaMeEventLoader(item=Event(type='sitting'), selector=content)
        l.add_value('name', response.meta['name'])
        sitting_id = parse_qs(response.url)['sjednicaid']
        l.add_value('identifier', sitting_id)
        l.add_xpath(
            'start_date',
            ".//tr[td//text()[contains(., 'Datum')]]/td[2]//text()"
        )
        l.add_xpath(
            'end_date',
            ".//tr[td//text()[contains(., 'Datum')]]/td[2]//text()"
        )
        l.add_value('sources', [response.url])
        sitting = l.load_item()
        yield sitting

        speeches = content.xpath(
            ".//li[text()[contains(., 'Autorizovani fonografski zapis.')]]")
        if speeches:
            url = speeches.xpath('.//a/@href').extract()[0]
            l = SkupstinaMeSpeechLoader(item=Speech(), selector=speeches)
            l.add_value('event_id', sitting['identifier'])
            l.add_xpath(
                'title',
                ".//tr[td//text()[contains(., 'Opis')]]/td[2]//text()")
            l.add_value('sources', [urljoin(response.url, url)])
            yield l.load_item()
