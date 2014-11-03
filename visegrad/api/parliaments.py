# -*- coding: utf8 -*-
from visegrad.api.base import VisegradApiExport


class SkustinaMeApiExport(VisegradApiExport):
    parliament = 'me/skupstina-test'
    parliament_code = 'ME_SKUPSTINA'
    domain = 'skupstina.me'

    def make_chamber(self):
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

    def make_chamber(self):
        chamber = {
            'classification': 'chamber',
            'identifiers': [
                {'identifier': '40', 'scheme': 'parlament.hu/chamber'}
            ],
            'name': u'Országgyűlés 2014 - ',
        }

        return self.get_or_create('organizations', chamber)


class SejmPlApiExport(VisegradApiExport):
    parliament = 'pl/sejm'
    parliament_code = 'PL_SEJM'
    domain = 'mojepanstwo.pl'

    def make_chamber(self):
        chamber = {
            'classification': 'chamber',
            'identifiers': [
                {'identifier': '40', 'scheme': 'mojepanstwo.pl/chamber'}
            ],
            'name': u'Sejm 2011 - ',
        }

        return self.get_or_create('organiztions', chamber)
