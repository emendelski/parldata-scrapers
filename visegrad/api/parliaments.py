# -*- coding: utf8 -*-
from visegrad.api.base import VisegradApiExport


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
                'name': u'1990 - 94',
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
