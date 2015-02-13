# -*- coding: utf-8 -*-
import scrapy
from scrapy.exceptions import DropItem

from urllib import urlencode
from urlparse import urlparse

import json

import uuid

from visegrad.spiders import VisegradSpider
from visegrad.loaders import MojePanstwoPersonLoader, OrganizationLoader, \
    MojePanstwoMembershipLoader, MojePanstwoVoteEventLoader, \
    MojePanstwoVoteLoader, MojePanstwoMotionLoader, MojePanstwoSpeechLoader
from visegrad.items import Person, Organization, Membership, VoteEvent, Vote, \
    Motion, Count, Speech
from visegrad.api.parliaments import SejmPlApiExport


class MojepanstwoPlSpider(VisegradSpider):
    name = "mojepanstwo.pl"
    allowed_domains = ["api.mojepanstwo.pl"]
    api_url = 'http://api.mojepanstwo.pl/'
    page_limit = 100
    parliament_code = 'PL_SEJM'
    exporter_class = SejmPlApiExport

    def start_requests(self):
        yield scrapy.Request(
            self.get_api_url(
                '/dane/dataset/poslowie/search.json',
                limit=self.page_limit),
            callback=self.parse_people,
        )
        yield scrapy.Request(
            self.get_api_url(
                '/dane/dataset/sejm_komisje/search.json',
                limit=self.page_limit),
            callback=self.parse_committees,
        )
        yield scrapy.Request(
            self.get_api_url(
                '/dane/dataset/sejm_glosowania/search.json',
                limit=self.page_limit),
            callback=self.parse_vote_events,
        )
        yield scrapy.Request(
            self.get_api_url(
                '/dane/dataset/sejm_wystapienia/search.json',
                limit=self.page_limit),
            callback=self.parse_speeches
        )

    def parse_people(self, response):
        data = json.loads(response.body_as_unicode())
        people = data['search']['dataobjects']
        for person in people:
            yield scrapy.Request(
                self.get_api_url(
                    person['_id'],
                    layers='info'),
                callback=self.parse_person
            )

        pagination = data['search']['pagination']
        if pagination['to'] < pagination['total']:
            page = response.meta.get('page', 1) + 1
            yield scrapy.Request(
                self.get_api_url(
                    '/dane/dataset/poslowie/search.json',
                    page=page,
                    limit=self.page_limit),
                callback=self.parse_people,
                meta={'page': page}
            )

    def parse_person(self, response):
        data = json.loads(response.body_as_unicode())
        if data['object'] is False:
            name = response.meta.get('name')
            if name:
                l = MojePanstwoPersonLoader(item=Person(),
                    scheme='mojepanstwo.pl/people')
                l.add_value('name', name)
                l.add_value('identifiers', response.meta.get('id'))
                yield l.load_item()
                raise StopIteration()
            else:
                raise DropItem()

        person = data['object']['data']
        l = MojePanstwoPersonLoader(item=Person(),
            scheme='mojepanstwo.pl/people')
        l.add_value('name', person['poslowie.nazwa'])
        l.add_value('given_name', person['poslowie.imiona'])
        l.add_value('family_name', person['poslowie.nazwisko'])
        l.add_value('identifiers', person['poslowie.id'])
        l.add_value('birth_date', person['poslowie.data_urodzenia'])
        if person['ludzie.id']:
            l.add_value(
                'image',
                'http://resources.sejmometr.pl/mowcy/a/0/%s.jpg' % person['ludzie.id']
            )
        # l.add_value('sources', data['object']['_mpurl'])
        l.add_value(
            'sources',
            'http://mojepanstwo.pl/dane/poslowie/%s' % data['object']['id']
        )
        gender = person.get('poslowie.plec')
        gender = {
            'M': 'male',
            'K': 'female'
        }.get(gender)
        if gender:
            l.add_value('gender', gender)
        person_item = l.load_item()
        yield person_item

        p = OrganizationLoader(item=Organization(classification='party'),
            scheme='mojepanstwo.pl/parties')
        p.add_value('identifiers', person['sejm_kluby.id'])
        p.add_value('name', person['sejm_kluby.nazwa'])
        p.add_value('other_names', person['sejm_kluby.skrot'])
        party = p.load_item()
        yield party

        m = MojePanstwoMembershipLoader(item=Membership())
        m.add_value('person_id', person_item['identifiers'][0])
        m.add_value('organization_id', party['identifiers'][0])
        yield m.load_item()

        committees_memberships = data['object']['layers']['info']\
            ['komisje_stanowiska']

        for membership in committees_memberships:
            details = membership['s_poslowie_komisje']
            committee_id = details['komisja_id']

            m = MojePanstwoMembershipLoader(item=Membership())
            m.add_value('person_id', person_item['identifiers'][0])
            m.add_value('organization_id', {
                'scheme': 'mojepanstwo.pl/committees',
                'identifier': committee_id
            })
            m.add_value('start_date', details['od'])
            m.add_value('end_date', details['do'])
            yield m.load_item()

    def parse_committees(self, response):
        data = json.loads(response.body_as_unicode())
        committees = data['search']['dataobjects']

        for obj in committees:
            committee = obj['data']
            l = OrganizationLoader(item=Organization(classification='committee'),
                scheme='mojepanstwo.pl/committees')
            l.add_value('identifiers', committee['sejm_komisje.id'])
            l.add_value('name', committee['sejm_komisje.nazwa'])
            l.add_value('sources', obj['_mpurl'])
            yield l.load_item()

        pagination = data['search']['pagination']
        if pagination['to'] < pagination['total']:
            page = response.meta.get('page', 1) + 1
            yield scrapy.Request(
                self.get_api_url(
                    '/dane/dataset/sejm_komisje/search.json',
                    page=page,
                    limit=self.page_limit),
                callback=self.parse_people,
                meta={'page': page}
            )

    def parse_vote_events(self, response):
        data = json.loads(response.body_as_unicode())
        vote_events = data['search']['dataobjects']
        for vote_event in vote_events:
            yield scrapy.Request(
                self.get_api_url(
                    vote_event['_id'],
                    layers='*'),
                callback=self.parse_vote_event
            )

        pagination = data['search']['pagination']
        if pagination['to'] < pagination['total']:
            page = response.meta.get('page', 1) + 1
            yield scrapy.Request(
                self.get_api_url(
                    '/dane/dataset/sejm_glosowania/search.json',
                    page=page,
                    limit=self.page_limit),
                callback=self.parse_vote_events,
                meta={'page': page}
            )

    def parse_vote_event(self, response):
        data = json.loads(response.body_as_unicode())
        vote_event = data['object']['data']

        # link motion and vote event
        motion_id = str(uuid.uuid4())

        m = MojePanstwoMotionLoader(item=Motion(id=motion_id))
        m.add_value('text', vote_event['sejm_glosowania.tytul'])
        m.add_value('date', vote_event.get('sejm_glosowania.czas'))
        if vote_event['sejm_glosowania.wynik_id'] in ('1', '2'):
            m.add_value('result', vote_event['sejm_glosowania.wynik_id'])
        m.add_value('legislative_session_id',
            vote_event['sejm_posiedzenia.tytul'])
        # m.add_value('sources', data['object']['_mpurl'])
        m.add_value(
            'sources',
            'http://mojepanstwo.pl/dane/sejm_glosowania/%s' % data['object']['id']
        )
        motion_item = m.load_item()
        yield motion_item
        ve = MojePanstwoVoteEventLoader(item=VoteEvent(motion_id=motion_id))
        ve.add_value('identifier', vote_event['sejm_glosowania.id'])
        ve.add_value('start_date', vote_event.get('sejm_glosowania.czas'))
        if vote_event['sejm_glosowania.wynik_id'] in ('1', '2'):
            m.add_value('result', vote_event['sejm_glosowania.wynik_id'])
        ve.add_value('legislative_session_id',
            vote_event['sejm_posiedzenia.tytul'])
        counts = dict((
            ('yes', vote_event['sejm_glosowania.z']),
            ('no', vote_event['sejm_glosowania.p']),
            ('abstain', vote_event['sejm_glosowania.w']),
            ('absent', vote_event['sejm_glosowania.n']),
        ))
        counts = [
            Count(option=option, value=value) for option, value in counts.items()
        ]
        ve.add_value('counts', counts)
        vote_event_item = ve.load_item()
        yield vote_event_item
        votes = data['object']['layers']['wynikiIndywidualne']
        for vote in votes:
            v = MojePanstwoVoteLoader(
                item=Vote(),
                scheme='mojepanstwo.pl/people'
            )
            person_id = vote['poslowie']['id']
            v.add_value('vote_event_id', vote_event_item['identifier'])
            v.add_value('voter_id', person_id)
            v.add_value('option', vote['glosy']['glos_id'])
            yield v.load_item()
            yield scrapy.Request(
                self.get_api_url(
                    '/dane/poslowie/%s' % person_id,
                    layers='info'),
                callback=self.parse_person,
                meta={
                    'name': vote['poslowie'].get('nazwa'),
                    'id': person_id
                }
            )

    def parse_speeches(self, response):
        data = json.loads(response.body_as_unicode())
        speeches = data['search']['dataobjects']
        for speech in speeches:
            yield scrapy.Request(
                self.get_api_url(
                    speech['_id'],
                    layers='html'),
                callback=self.parse_speech
            )

        pagination = data['search']['pagination']
        if pagination['to'] < pagination['total']:
            page = response.meta.get('page', 1) + 1
            yield scrapy.Request(
                self.get_api_url(
                    '/dane/dataset/sejm_wystapienia/search.json',
                    page=page,
                    limit=self.page_limit),
                callback=self.parse_speeches,
                meta={'page': page}
            )

    def parse_speech(self, response):
        data = json.loads(response.body_as_unicode())
        speech = data['object']['data']
        l = MojePanstwoSpeechLoader(
            item=Speech(), scheme='mojepanstwo.pl/people')
        l.add_value('title', speech['sejm_wystapienia.tytul'])
        l.add_value('creator_id', speech['ludzie.posel_id'])
        l.add_value('text', data['object']['layers']['html'])
        l.add_value('date', speech['sejm_wystapienia.data'])
        video = speech['sejm_wystapienia.yt_id']
        if video and video != "0":
            video = "https://www.youtube.com/watch?v=%s" % video
        else:
            video = speech['sejm_wystapienia.video']
        if video == "0":
            video = ""
        l.add_value('video', video)
        l.add_value('sources', [data['object']['_mpurl']])
        yield l.load_item()

    def get_api_url(self, path, **params):
        url = self.api_url.rstrip('/')
        u = urlparse(path)
        path = u.path
        if not path.startswith('/'):
            path = '/' + path
        url += path
        if params:
            url += '?%s' % urlencode(params, True)
        return url
