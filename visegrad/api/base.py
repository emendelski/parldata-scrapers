from scrapy.conf import settings
import scrapy.log
from scrapy.log import INFO

import vpapi

import json

import os


class VisegradApiExport(object):
    parliament = ''
    domain = ''
    user = 'scraper'
    parliament_code = ''

    PEOPLE_FILE = 'Person.json'
    ORGANIZATIONS_FILE = 'Organization.json'
    MEMBERSHIPS_FILE = 'Membership.json'
    MOTIONS_FILE = 'Motion.json'
    VOTE_EVENTS_FILE = 'VoteEvent.json'
    VOTES_FILE = 'Vote.json'
    FILES = {
        'people': PEOPLE_FILE,
        'organizations': ORGANIZATIONS_FILE,
        'memberships': MEMBERSHIPS_FILE,
        'motions': MOTIONS_FILE,
        'vote-events': VOTE_EVENTS_FILE,
        'votes': VOTES_FILE,
    }

    def __init__(self, log = None):
        vpapi.parliament(self.get_parliament())
        vpapi.authorize(self.get_user(), self.get_password())

        self._chamber = None
        self._ids = {}
        if log is None:
            self.log = scrapy.log.msg
        else:
            self.log = log

    def get_parliament(self):
        return os.environ.get('VPAPI_PARLIAMENT_ENDPOINT', self.parliament)

    def get_user(self):
        return self.user

    def get_password(self):
        var = 'VPAPI_PWD_%s' % self.parliament_code.upper()
        return os.environ.get(var)

    def run_export(self):
        self.log('Exporting people', INFO)
        self.export_people()
        self.log('Exporting organizations', INFO)
        self.export_organizations()
        self.log('Exporting memberships', INFO)
        self.export_memberships()
        self.log('Exporting motions', INFO)
        self.export_motions()
        self.log('Exporting votes', INFO)
        self.export_votes()

    def load_json(self, source):
        filename = os.path.join(
            settings.get('OUTPUT_PATH', ''),
            self.domain,
            self.FILES[source]
        )
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                for i in f:
                    line = i.lstrip('[').rstrip().rstrip(']').rstrip(',')
                    yield json.loads(line)

    def get_or_create(self, endpoint, item):
        sort = []
        if endpoint == 'memberships':
            where = {
                'person_id': item['person_id'],
                'organization_id': item['organization_id']
            }
            if 'start_date' in item:
                where['start_date'] = item['start_date']
            sort = [('start_date', -1)]
        elif endpoint == 'motions':
            where = {'sources.url': item['sources'][0]['url']}
        elif endpoint == 'vote-events':
            where = {'start_date': item['start_date']}
        elif endpoint == 'votes':
            where = {
                'vote_event_id': item['vote_event_id'],
                'voter_id': item['voter_id'],
            }
        else:
            where = {
                'identifiers': {'$elemMatch': item['identifiers'][0]}}
        created = False
        resp = vpapi.get(endpoint, where=where, sort=sort)
        if not resp['_items']:
            resp = vpapi.post(endpoint, item)
            created = True
        else:
            pk = resp['_items'][0]['id']
            resp = vpapi.put("%s/%s" % (endpoint, pk), item)

        if resp['_status'] != 'OK':
            raise Exception(resp)
        resp['_created'] = created
        return resp

    def batch_create(self, endpoint, items):
        return vpapi.post(endpoint, items)

    def get_remote_id(self, scheme, identifier):
        key = "%s/%s" % (scheme, identifier)
        if key in self._ids:
            return self._ids[key]

        domain, category = scheme.split('/')
        if category in ('committees', 'parties', 'chamber'):
            endpoint = 'organizations'
        else:
            endpoint = category

        resp = vpapi.get(endpoint, where={
            'identifiers': {
                '$elemMatch': {'scheme': scheme, 'identifier': identifier}
            }
        })

        if resp['_items']:
            item = resp['_items'][0]
            self._ids[key] = item['id']
            return item['id']

    def make_chamber(self):
        raise NotImplementedError()

    def get_chamber(self):
        if not self._chamber:
            self._chamber = self.make_chamber()
        return self._chamber

    def export_people(self):
        chamber = self.get_chamber()
        people = self.load_json('people')

        for person in people:
            resp = self.get_or_create('people', person)

    def export_organizations(self):
        chamber = self.get_chamber()
        organizations = self.load_json('organizations')

        for organization in organizations:
            organization['parent_id'] = chamber['id']
            resp = self.get_or_create('organizations', organization)

    def export_memberships(self):
        memberships = self.load_json('memberships')

        for item in memberships:
            person_id = self.get_remote_id(
                scheme=item['person_id']['scheme'],
                identifier=item['person_id']['identifier'])
            organization_id = self.get_remote_id(
                scheme=item['organization_id']['scheme'],
                identifier=item['organization_id']['identifier'])
            item['person_id'] = person_id
            item['organization_id'] = organization_id
            self.get_or_create('memberships', item)

    def export_motions(self):
        chamber = self.get_chamber()
        motions = self.load_json('motions')

        for item in motions:
            item['organization_id'] = chamber['id']
            resp = self.get_or_create('motions', item)

    def export_votes(self):
        vote_events = self.load_json('vote-events')

        for vote_event in vote_events:
            local_identifier = vote_event['identifier']
            del vote_event['identifier']
            vote_event_resp = self.get_or_create('vote-events', vote_event)
            filtered_votes = filter(
                lambda x: x['vote_event_id'] == local_identifier,
                self.load_json('votes')
            )
            # send votes only once, when vote event is created
            if vote_event_resp['_created']:
                for vote in filtered_votes:
                    vote['vote_event_id'] = vote_event_resp['id']
                    vote['voter_id'] = self.get_remote_id(
                        scheme=vote['voter_id']['scheme'],
                        identifier=vote['voter_id']['identifier'])
                if filtered_votes:
                    self.batch_create('votes', filtered_votes)
