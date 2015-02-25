# -*- coding: utf8 -*-
import scrapy
from scrapy.contrib.loader.processor import TakeFirst
from scrapy.conf import settings

from urlparse import urlparse, parse_qs, urljoin

from urllib import urlencode

from datetime import date, datetime, timedelta

import re

from visegrad.spiders import VisegradSpider
from visegrad.items import Person, Vote, VoteEvent, Organization, Membership,\
    Motion, Count, Speech
from visegrad.loaders import PersonLoader, ParlamentHuVoteLoader, ParlamentHuVoteEventLoader,\
    ParlamentHuOrganizationLoader, ParlamentHuMembershipLoader, ParlamentHuMotionLoader, \
    CountLoader, ParlamentHuSpeechLoader
from visegrad.api.parliaments import ParlamentHuApiExport
from visegrad.utils import parse_hu_name


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


class ParlamentHu(VisegradSpider):
    name = 'parlament.hu'
    allowed_domains = ['parlament.hu']
    parliament_code = 'HU_ORSZAGGYULES'
    exporter_class = ParlamentHuApiExport

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
    VOTES_START_DATE = date(2014, 4, 26)

    PARLIAMENTS_IDENTIFIERS = {
        '2014-': {
            'scheme': 'parlament.hu/chamber',
            'identifier': '40'
        },
        '2010-2014': {
            'scheme': 'parlament.hu/chamber',
            'identifier': '39'
        },
        '2006-2010': {
            'scheme': 'parlament.hu/chamber',
            'identifier': '38'
        },
        '2002-2006': {
            'scheme': 'parlament.hu/chamber',
            'identifier': '37'
        },
        '1998-2002': {
            'scheme': 'parlament.hu/chamber',
            'identifier': '36'
        },
        '1994-98': {
            'scheme': 'parlament.hu/chamber',
            'identifier': '35'
        },
        '1990-94': {
            'scheme': 'parlament.hu/chamber',
            'identifier': '34'
        }
    }

    def start_requests(self):
        yield scrapy.Request(self.PARTIES_URL, callback=self.parse_parties)
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
        return settings.get('HU_ORSZAGGYULES_ACCESS_TOKEN', '')

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
        query = parse_qs(urlparse(response.url).query)
        query_dict = dict(
            (k.lower(), ''.join(v).upper())
            for k, v in query.iteritems()
        )
        committee_id = 'p_biz=%(p_biz)s&p_ckl=%(p_ckl)s' % query_dict
        year = query_dict['p_ckl']

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
        l.add_value('parent_id', {
            'scheme': 'parlament.hu/chamber',
            'identifier': year
        })
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
        links = parties.css('a::attr(href)').extract()
        for link in map(get_action_url, links):
            yield scrapy.Request(link, callback=self.parse_people)

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
        name = response.xpath('//nev/text()').extract()[0]
        splitted_name = parse_hu_name(name)
        l.add_value(None, splitted_name)
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
            l.add_value('organization_id', party['identifiers'])
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
                l.add_value('organization_id', party['identifiers'])
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

        terms_of_service = response.css('#valasztas').xpath(".//tr[td]")
        if terms_of_service:
            header = response.css('#valasztas').xpath(
                './/tr[th][2]/th/text()').extract()
            try:
                year_index = header.index('Ciklus')
                start_date_index = header.index(u'Mand\xe1tum kezdete')
                end_date_index = header.index(u'Mand\xe1tum v\xe9ge')
            except ValueError:
                terms_of_service = []

            for term in terms_of_service:
                row = map(unicode.strip, term.css('td::text').extract())
                parliament_id = self.PARLIAMENTS_IDENTIFIERS[row[year_index]]

                m = ParlamentHuMembershipLoader(item=Membership())
                m.add_value('person_id', person['identifiers'])
                m.add_value('organization_id', parliament_id)
                m.add_value('start_date', row[start_date_index])
                m.add_value('end_date', row[end_date_index])
                yield m.load_item()

        committees = response.css('#biz-tagsag').xpath('.//tr[td]')
        for committee in committees:
            url = committee.xpath('.//a/@href').extract()[0]
            url = urljoin(response.url, url)
            yield scrapy.Request(url, callback=self.parse_commitee)

            query = parse_qs(urlparse(url).query)
            committee_id = 'p_biz=%(p_biz)s&p_ckl=%(p_ckl)s' % dict(
                (k.lower(), ''.join(v).upper())
                for k, v in query.iteritems()
            )

            m = ParlamentHuMembershipLoader(item=Membership(), selector=committee)
            m.add_value('person_id', person['identifiers'])
            m.add_value(
                'organization_id',
                {
                    'identifier': committee_id,
                    'scheme': 'parlament.hu/committees'
                }
            )
            m.add_xpath('role', './/td[3]/text()')
            m.add_xpath('start_date', './/td[4]/text()')
            m.add_xpath('end_date', './/td[5]/text()')
            yield m.load_item()

        speeches = response.css('#felszolalasok')
        speeches_urls = speeches.xpath('.//tr/td/a/@href').re(r'.*p_ckl=40.*')
        for speech in speeches_urls:
            yield scrapy.Request(
                urljoin(response.url, speech),
                callback=self.parse_person_speeches
            )

        yield person

    def parse_person_speeches(self, response):
        content = response.xpath('//table[3]')

        sessions = content.xpath('./tr[1]//table//tr//td[1]//a')

        stop_date = self.get_latest_speech_date()

        for session in sessions:
            dt = session.re(r'\d{4}\.\d{2}.\d{2}')
            if dt:
                dt = datetime.strptime(dt[0], '%Y.%m.%d').date()
                if stop_date and dt < stop_date:
                    raise StopIteration()
            url = session.xpath('.//@href').extract()[0]
            yield scrapy.Request(
                urljoin(response.url, url),
                callback=self.parse_session_speeches
            )

        next_page = content.xpath(
            "./tr[2]//a[contains(text(), '>>')]/@href").extract()
        for page in next_page:
            yield scrapy.Request(
                urljoin(response.url, page),
                callback=self.parse_person_speeches
            )

    def parse_session_speeches(self, response):
        content = response.css('.pair-content table')
        speeches = content.xpath('.//tr')
        for speech in speeches:
            url = speech.xpath('.//td[1]//a/@href').extract()
            if url:
                yield scrapy.Request(
                    urljoin(response.url, url[0]),
                    callback=self.parse_speech,
                    meta={
                        'time': speech.xpath('.//td[5]/text()').re(
                            r"\d{2}\:\d{2}\:\d{2}")
                    }
                )

    def parse_speech(self, response):
        paragraphs = response.css('p[class^="P"]')
        text = '\n'.join(
            ''.join(
                span.xpath('.//text()').extract()
            ) for span in paragraphs
        ).strip()

        l = ParlamentHuSpeechLoader(item=Speech(), selector=response,
            scheme='parlament.hu/people')
        l.add_value('text', text)
        l.add_value('type', 'speech')
        l.add_value('sources', [response.url])
        l.add_xpath('position', '//b[1]/text()')
        l.add_xpath('video', '//table//tr[6]//td[2]/a/@href')
        l.add_xpath('creator_id', '//table//tr[2]//td[2]/a/@href',
            re=r'ogy_kpv\.kepv_adat\?p_azon=(\w\d+)')

        date = response.xpath(
            '//table//tr[1]/th/text()').re(r'\d{4}\.\d{2}.\d{2}\.')
        time = response.meta.get('time')
        if date:
            date = date[0]
            if time:
                date += time[0]
            l.add_value('date', date)
        item = l.load_item()
        yield item
        if 'creator_id' in item:
            yield scrapy.Request(self.get_api_url(
                self.PERSON_ENDPOINT, params={
                    'p_azon': item['creator_id']['identifier']}),
                callback=self.parse_person, meta={
                    'p_azon': item['creator_id']['identifier']})

    def get_votes_requests(self):
        start = date.today() - timedelta(days = 60)
        end = date.today()
        get_votes_url = lambda start, end: self.get_api_url(
            self.VOTES_ENDPOINT, params={
                'p_datum_tol': start.strftime("%Y.%m.%d"),
                'p_datum_ig': end.strftime("%Y.%m.%d")
            })

        stop_date = self.VOTES_START_DATE
        if settings.get('CRAWL_LATEST_ONLY'):
            stop_date = self.get_latest_vote_event_date() or stop_date

        while start > stop_date:
            yield scrapy.Request(
                get_votes_url(start, end), callback=self.parse_votes)
            end = start - timedelta(days = 1)
            start = end - timedelta(days = 60)

        start = stop_date
        if start <= end:
            yield scrapy.Request(
                get_votes_url(start, end), callback=self.parse_votes)

    def parse_votes(self, response):
        VOTE_URL = 'http://www.parlament.hu/internet/cplsql/ogy_szav.szav_lap_egy?\
p_szavdatum=%s&p_szavkepv=I&p_szavkpvcsop=I&p_ckl=40'
        for voting in response.xpath('//szavazas'):
            voting_id = voting.xpath('./@idopont').extract()[0]

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
                count = c.load_item()
                if count.get('value'):
                    counts.append(count)

            l.add_value('counts', counts)
            l.add_value('sources', VOTE_URL % voting_id)

            yield l.load_item()

            motions = voting.xpath('.//inditvanyok/inditvany')
            for motion in motions:
                m = ParlamentHuMotionLoader(item=Motion(), selector=motion)
                m.add_xpath('text', './/cim/text()')
                m.add_value(
                    'requirement',
                    voting.xpath(
                        u'.//tulajdonsag[@nev="Szavazási mód"]/@ertek').extract()
                )
                m.add_value('sources', VOTE_URL % voting_id)
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
            lambda x: x.css('a') and not x.css('th'),
            response.css('#szav-nev-szerint tr'))
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
                self.PERSON_ENDPOINT, params={
                    'p_azon': item['voter_id']['identifier']}),
                callback=self.parse_person, meta={
                    'p_azon': item['voter_id']['identifier']})

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
