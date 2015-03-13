# -*- coding: utf8 -*-
from scrapy.log import DEBUG

import tempfile

import re

import requests

import vpapi

from visegrad.api.base import VisegradApiExport
from visegrad.utils import parse_me_pdf


class SkustinaMeApiExport(VisegradApiExport):
    parliament = 'me/skupstina'
    parliament_code = 'ME_SKUPSTINA'
    domain = 'skupstina.me'

    def make_chamber(self, index):
        chamber = {
            'classification': 'chamber',
            'identifiers': [
                {'identifier': '25', 'scheme': 'skupstina.me/chamber'}
            ],
            'name': u'Skupština Crne Gore 2012 - 2015',
        }

        return self.get_or_create('organizations', chamber)

    def export_speeches(self):
        speeches = self.load_json('speeches')
        people = {}
        titles_regex = re.compile(
            r'([dD]r )|(mr )|(doc\. )|(Prof\. )|(Prim\.)')
        spaces_regex = re.compile(r'\s{2,}')
        prefix_regex = re.compile(
            ur'(pred\u015bedavaju\u0107i )|(pred\u015bednik )|\
(generalni sekretar )', re.U)

        for p in vpapi.getall('people'):
            name = titles_regex.sub('', p['name'])
            name = name.replace('-', ' ')
            name = spaces_regex.sub(' ', name).lower()
            people[name] = p['id']

        for speech in speeches:
            session_id = speech.get('event_id')
            speech['event_id'] = self.events_ids[session_id]
            url = speech['sources'][0]['url']
            if url.endswith('.pdf'):
                parsed_speeches = self.download_pdf(url)
                for n, s in enumerate(parsed_speeches):
                    text_speech = speech.copy()
                    text_speech['text'] = s['text']
                    text_speech['position'] = n + 1
                    text_speech['type'] = 'speech'

                    creator = s['creator'].lower()
                    creator = prefix_regex.sub('', creator)
                    creator = creator.replace('-', ' ')
                    creator = spaces_regex.sub(' ', creator)
                    if creator in people:
                        text_speech['creator_id'] = people[creator]

                    self.get_or_create(
                        'speeches',
                        text_speech,
                        where_keys=['event_id', 'position']
                    )
            else:
                self.get_or_create('speeches', speech)

    def download_pdf(self, url):
        pdf_file = tempfile.NamedTemporaryFile()
        self.log('Dowloading file', DEBUG)
        r = requests.get(url, stream=True)
        with pdf_file:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    pdf_file.write(chunk)
                    pdf_file.flush()
            r.close()
            self.log('Parsing file', DEBUG)
            for i in parse_me_pdf(pdf_file.name):
                yield i


class ParlamentHuApiExport(VisegradApiExport):
    parliament = 'hu/orszaggyules'
    parliament_code = 'HU_ORSZAGGYULES'
    domain = 'parlament.hu'
    single_chamber = False

    def make_chamber(self, index):
        chambers = [
            {
                'identifiers': [
                    {'identifier': '40', 'scheme': 'parlament.hu/chamber'}
                ],
                'name': u'Országgyűlés 2014 - ',
            },
            {
                'identifiers': [
                    {'identifier': '39', 'scheme': 'parlament.hu/chamber'}
                ],
                'name': u'Országgyűlés 2010 - 2014',
            },
            {
                'identifiers': [
                    {'identifier': '38', 'scheme': 'parlament.hu/chamber'}
                ],
                'name': u'Országgyűlés 2006 - 2010',
            },
            {
                'identifiers': [
                    {'identifier': '37', 'scheme': 'parlament.hu/chamber'}
                ],
                'name': u'Országgyűlés 2002 - 2006',
            },
            {
                'identifiers': [
                    {'identifier': '36', 'scheme': 'parlament.hu/chamber'}
                ],
                'name': u'Országgyűlés 1998 - 2002',
            },
            {
                'identifiers': [
                    {'identifier': '35', 'scheme': 'parlament.hu/chamber'}
                ],
                'name': u'Országgyűlés 1994 - 98',
            },
            {
                'identifiers': [
                    {'identifier': '34', 'scheme': 'parlament.hu/chamber'}
                ],
                'name': u'Országgyűlés 1990 - 94',
            }
        ]

        results = []

        for ch in chambers:
            ch['classification'] = 'chamber'
            results.append(self.get_or_create('organizations', ch))
        return results[index]


class SejmPlApiExport(VisegradApiExport):
    parliament = 'pl/sejm'
    parliament_code = 'PL_SEJM'
    domain = 'mojepanstwo.pl'

    def make_chamber(self, index):
        chamber = {
            'classification': 'chamber',
            'identifiers': [
                {'identifier': '7', 'scheme': 'mojepanstwo.pl/chamber'}
            ],
            'name': u'Sejm 2011 - ',
        }

        return self.get_or_create('organizations', chamber)
