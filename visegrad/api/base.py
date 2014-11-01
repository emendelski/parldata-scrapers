import vpapi

import json

import logging

import os


logging.basicConfig()


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

    def __init__(self):
        vpapi.parliament(self.parliament)
        vpapi.authorize(self.get_user(), self.get_password())

        self._chamber = None
        self._ids = {}
        self.log = logging.getLogger(__name__)
        self.log.setLevel(logging.INFO)

    def get_user(self):
        return self.user

    def get_password(self):
        var = 'VPAPI_PWD_%s' % self.parliament_code.upper()
        return os.environ.get(var)

    def run_export(self):
        self.log.info('Exporting people')
        self.export_people()
        self.log.info('Exporting organizations')
        self.export_organizations()
        self.log.info('Exporting memberships')
        self.export_memberships()
        self.log.info('Exporting motions')
        self.export_motions()
        self.log.info('Exporting votes')
        self.export_votes()

    def load_json(self, source):
        filename = os.path.join(self.domain, self.FILES[source])
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                return json.loads(f.read())
        return []

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
        resp = vpapi.get(endpoint, where=where, sort=sort)
        if not resp['_items']:
            resp = vpapi.post(endpoint, item)
        else:
            pk = resp['_items'][0]['id']
            resp = vpapi.put("%s/%s" % (endpoint, pk), item)

        if resp['_status'] != 'OK':
            raise Exception(resp)
        return resp

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
        votes = self.load_json('votes')

        for vote_event in vote_events:
            local_identifier = vote_event['identifier']
            del vote_event['identifier']
            resp = self.get_or_create('vote-events', vote_event)
            filtered_votes = filter(
                lambda x: x['vote_event_id'] == local_identifier, votes)
            for x in filtered_votes:
                votes.remove(x)
            for vote in filtered_votes:
                vote['vote_event_id'] = resp['id']
                vote['voter_id'] = self.get_remote_id(
                    scheme=vote['voter_id']['scheme'],
                    identifier=vote['voter_id']['identifier'])
                vote_resp = self.get_or_create('votes', vote)
