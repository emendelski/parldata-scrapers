# -*- coding: utf8 -*-
import scrapy
from scrapy.contrib.loader.processor import TakeFirst

from urlparse import urlparse, parse_qs, urljoin

from urllib import urlencode

from datetime import date, timedelta

import re

import os

from visegrad.items import Person, Vote, VoteEvent, Organization, Membership,\
    Motion, Count
from visegrad.loaders import PersonLoader, ParlamentHuVoteLoader, ParlamentHuVoteEventLoader,\
    ParlamentHuOrganizationLoader, ParlamentHuMembershipLoader, ParlamentHuMotionLoader, \
    CountLoader


def get_action_url(url):
    parsed_url = urlparse(url)
    query = parse_qs(parsed_url.query)
    key = filter(lambda x: x.endswith('pairAction'), query)
    if key:
        return "%(scheme)s://%(hostname)s%(path)s" % {
            'scheme': parsed_url.scheme,
            'hostname': parsed_url.hostname,
            'path': query[key[0]][0]
        }


class ParlamentHu(scrapy.Spider):
    name = 'parlament.hu'
    allowed_domains = ['parlament.hu']
    PEOPLE_ENDPOINT = 'kepviselok'
    PERSON_ENDPOINT = 'kepviselo'
    VOTES_ENDPOINT = 'szavazasok'
    COMMITTEES_URL = 'http://www.parlament.hu/az-orszaggyules-bizottsagai'
    PARTIES_URL = 'http://www.parlament.hu/a-partok-kepviselocsoportjai-es-a-\
fuggetlen-kepviselok-aktualis-'
    PARTIES_ARCHIVE_URL = 'http://www.parlament.hu/a-partok-\
kepviselocsoportjai-es-a-fuggetlen-kepviselok-1990-'
    API_URL = 'http://www.parlament.hu/cgi-bin/web-api/'
    API_MAPPINGS = {
        r'/internet/cplsql/ogy_kpv\.kepv_adat\?p_azon=(?P<p_azon>\w\d+)':
        PERSON_ENDPOINT,
    }
    VOTES_START_DATE = date(2014, 6, 1)

    def start_requests(self):
        yield scrapy.Request(self.PARTIES_URL, callback=self.parse_parties)
        yield scrapy.Request(
            self.PARTIES_ARCHIVE_URL, callback=self.parse_parties_archive)
        yield scrapy.Request(self.COMMITTEES_URL, callback=self.parse_commitees)
        for req in self.get_votes_requests():
            yield req

    def get_url(self, url):
        for regex in self.API_MAPPINGS:
            match = re.search(regex, url)
            if match:
                endpoint = self.API_MAPPINGS[regex]
                params = match.groupdict()
                return self.get_api_url(endpoint, params)
        return url

    def get_access_token(self):
        return os.environ.get('HU_ORSZAGGYULES_ACCESS_TOKEN', '')

    def get_api_url(self, endpoint, params = None):
        query = {'access_token': self.get_access_token()}
        if params:
            query.update(params)

        return "%(api_url)s%(endpoint)s.cgi?%(query)s" % {
            'api_url': self.API_URL,
            'endpoint': endpoint,
            'query': urlencode(query)
        }

    def parse_commitees(self, response):
        links = response.css(
            '.pair-content .pair-content table.table td a::attr(href)').extract()

        for link in map(get_action_url, links):
            yield scrapy.Request(link, self.parse_commitee)

    def parse_commitee(self, response):
        committee_id = urlparse(response.url).query

        content = response.css('.pair-content')

        l = ParlamentHuOrganizationLoader(
            item=Organization(classification='committee'), selector=content,
            scheme='parlament.hu/committees')
        l.add_value('identifiers', committee_id)
        l.add_css('name', 'th font::text')
        l.add_xpath(
            'founding_date', u'.//tr[contains(td[1], "Létrehozás")]/td[2]/text()')
        l.add_xpath(
            'dissolution_date', u'.//tr[contains(td[1], "Megszűnés")]/td[2]/text()')
        l.add_value('sources', [response.url])
        yield l.load_item()

    def parse_parties(self, response):
        parties = response.css('.pair-content .pair-content table tr')
        for party in parties:
            short_name = party.xpath('.//td[2]/a/text()').extract()
            if short_name:
                long_name = party.xpath('.//td[3]/b/text()').extract()
                p = self.get_party(
                    TakeFirst()(short_name), TakeFirst()(long_name))
                if p:
                    yield p

    def parse_parties_archive(self, response):
        links = response.css(
            '.pair-content .pair-content table a::attr(href)').extract()
        for link in map(get_action_url, links):
            yield scrapy.Request(link, callback=self.parse_people)

    def parse_people(self, response):
        links = response.css(
            '.pair-content table.table a::attr(href)').extract()
        for link in links:
            query = urlparse(link).query
            id = parse_qs(query).get('p_azon')[0]
            url = self.get_api_url(self.PERSON_ENDPOINT, {'p_azon': id})
            yield scrapy.Request(
                url, callback=self.parse_person, meta={'p_azon': id})

    def parse_person(self, response):
        l = PersonLoader(item=Person(), response=response,
            scheme='parlament.hu/people')
        pk = response.meta['p_azon']
        l.add_value('identifiers', pk)
        l.add_xpath('name', '//nev/text()')
        l.add_xpath('email', '//email/text()')
        l.add_xpath('links', '//honlap/text()')
        person_url = 'http://www.parlament.hu/internet/cplsql/ogy_kpv.\
kepv_adat?p_azon=%s' % pk
        l.add_value('sources', [person_url])
        person = l.load_item()

        yield scrapy.Request(
            person_url, callback=self.parse_person_details,
            meta={'item': person})

        memberships = response.xpath('//kepvcsop-tisztsegek/tisztseg | \
//kepvcsop-tagsagok/tagsag')
        for m in memberships:
            party = self.get_party(m.xpath('./@kepvcsop').extract()[0])
            if not party:
                continue
            yield party

            l = ParlamentHuMembershipLoader(item=Membership(), selector=m)
            l.add_value('person_id', person['identifiers'])
            l.add_xpath('organization_id', './@kepvcsop')
            l.add_xpath('start_date', './@tol_datum')
            l.add_xpath('end_date', './@ig_datum')
            if m.xpath('name()').extract()[0] == 'tisztseg':
                l.add_xpath('role', './@funkcio')
            l.add_value('sources', [person_url])
            yield l.load_item()

        functions = response.xpath('//kepvcsop-tisztsegek')
        for f in functions:
            party = self.get_party(m.xpath('./@kepvcsop').extract()[0])

            l = ParlamentHuMembershipLoader(item=Membership(), selector=m)
            l.add_value('person_id', person['identifiers'])
            if party:
                l.add_xpath('organization_id', './@kepvcsop')
            l.add_xpath('start_date', './@tol_datum')
            l.add_xpath('end_date', './@ig_datum')
            l.add_value('sources', [person_url])
            yield l.load_item()

    def parse_person_details(self, response):
        item = response.meta['item']
        l = PersonLoader(item=item, response=response,
            response_url=response.url)
        l.add_css('image', 'img.kepviselo-foto::attr(src)')
        person = l.load_item()

        committees = response.css('#biz-tagsag').xpath('.//tr[td]')
        for committee in committees:
            url = committee.xpath('.//a/@href').extract()[0]
            url = urljoin(response.url, url)
            yield scrapy.Request(url, callback=self.parse_commitee)

            committee_id = urlparse(url).query

            m = ParlamentHuMembershipLoader(item=Membership(), selector=committee)
            m.add_value('person_id', person['identifiers'])
            m.add_value('organization_id', committee_id)
            m.add_xpath('role', './/td[3]/text()')
            m.add_xpath('start_date', './/td[4]/text()')
            m.add_xpath('end_date', './/td[5]/text()')
            yield m.load_item()

        yield person

    def get_votes_requests(self):
        start = date.today() - timedelta(days = 60)
        end = date.today()
        get_votes_url = lambda start, end: self.get_api_url(
            self.VOTES_ENDPOINT, params={
                'p_datum_tol': start.strftime("%Y.%m.%d"),
                'p_datum_ig': end.strftime("%Y.%m.%d")
            })
        while start > self.VOTES_START_DATE:
            yield scrapy.Request(
                get_votes_url(start, end), callback=self.parse_votes)
            end = start - timedelta(days = 1)
            start = end - timedelta(days = 60)

        start = self.VOTES_START_DATE
        if start < end:
            yield scrapy.Request(
                get_votes_url(start, end), callback=self.parse_votes)

    def parse_votes(self, response):
        VOTE_URL = 'http://parlament.hu/internet/cplsql/ogy_szav.szav_lap_egy?\
p_szavdatum=%s&p_szavkepv=I&p_szavkpvcsop=I&p_ckl=40'
        for voting in response.xpath('//szavazas'):
            l = ParlamentHuVoteEventLoader(item=VoteEvent(), selector=voting)
            l.add_xpath('identifier', './@idopont')
            l.add_xpath('start_date', './@idopont')
            l.add_xpath('result', u'.//tulajdonsag[@nev="Elfogadás"]/@ertek')

            count_info = (
                ('yes', u'"Igen"-ek száma'),
                ('no', u'"Nem"-ek száma'),
                ('abstain', u'Tartózkodások'),
            )
            counts = []
            for option, tag_attr in count_info:
                c = CountLoader(item=Count(),
                    selector=voting.xpath('.//tulajdonsagok'))
                c.add_value('option', option)
                xpath = u'.//tulajdonsag[@nev=\'%s\']/@ertek' % tag_attr
                c.add_xpath('value', xpath)
                counts.append(c.load_item())
            l.add_value('counts', counts)

            yield l.load_item()
            voting_id = voting.xpath('./@idopont').extract()[0]

            motions = voting.xpath('.//inditvanyok/inditvany')
            for motion in motions:
                m = ParlamentHuMotionLoader(item=Motion(), selector=motion)
                m.add_xpath('text', './/cim/text()')
                m.add_value(
                    'requirement',
                    voting.xpath(
                        u'.//tulajdonsag[@nev="Szavazási mód"]/@ertek').extract()
                )
                yield m.load_item()

            yield scrapy.Request(
                VOTE_URL % voting_id, callback=self.parse_vote_page,
                meta={'voting_id': voting_id})

    def parse_vote_page(self, response):
        motions = response.css(
            '#szav-inditvanyok tr').xpath('.//td[1]/a/@href').extract()
        for m in motions:
            yield scrapy.Request(
                urljoin(response.url, m), callback=self.parse_motion)

        votes = filter(
            lambda x: not x.css('th'), response.css('#szav-nev-szerint tr'))
        for tr in votes:
            l = ParlamentHuVoteLoader(item=Vote(), selector=tr,
                scheme='parlament.hu/people')
            l.add_value('vote_event_id', response.meta['voting_id'])
            l.add_xpath('voter_id', './/td[1]/a/@href',
                        re=r'ogy_kpv\.kepv_adat\?p_azon=(\w\d+)')
            l.add_xpath('option', './/td[2]/text()')
            item = l.load_item()
            yield item
            yield scrapy.Request(self.get_api_url(
                self.PERSON_ENDPOINT, params={'p_azon': item['voter_id']}),
                callback=self.parse_person, meta={'p_azon': item['voter_id']})

    def parse_motion(self, response):
        pass

    def get_party(self, short_name, long_name = None):
        if short_name.strip().lower() == u"független":
            return None

        l = ParlamentHuOrganizationLoader(
            item=Organization(classification='party'),
            scheme='parlament.hu/parties')
        l.add_value('name', short_name)
        l.add_value('identifiers', short_name)
        if long_name:
            l.add_value('other_names', long_name)
        return l.load_item()
