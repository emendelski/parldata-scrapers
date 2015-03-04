# -*- coding: utf8 -*-
from scrapy.contrib.loader import ItemLoader
from scrapy.contrib.loader.processor import TakeFirst, MapCompose, Compose

from datetime import datetime

import pytz

from visegrad.utils import MakeList


def translate(phrase, words_dict, allow_empty=False):
    for key in words_dict:
        if phrase.strip().lower() == key.lower():
            return words_dict[key]
    if not allow_empty:
        return phrase


def strip(s):
    if type(s) in (unicode, str):
        return s.replace(u'\xa0', u' ').strip()
    return s


DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'


def local_to_utc(dt, timezone):
    tz = pytz.timezone(timezone)
    dt = tz.localize(dt)
    dt = dt.astimezone(pytz.utc)
    return dt


me_to_datetime = lambda d: datetime.strptime(strip(d).rstrip('.'), '%d.%m.%Y')
me_to_iso = lambda d: me_to_datetime(d).date().isoformat()
me_to_iso_datetime = lambda d: me_to_datetime(d).isoformat()
me_date_range = lambda d: map(me_to_datetime, d.split(';'))
me_start_date = lambda d: min(me_date_range(d)).isoformat()
me_end_date = lambda d: max(me_date_range(d)).isoformat()


def hu_to_iso(d):
    d = strip(d).rstrip('.')
    if not len(d):
        return d
    return datetime.strptime(d, '%Y.%m.%d').date().isoformat()


def hu_to_iso_datetime(d):
    d = strip(d).rstrip('.')
    if not len(d):
        return d
    try:
        d = datetime.strptime(d, '%Y.%m.%d.%H:%M:%S')
    except ValueError:
        d = d.rstrip('.')
        d = datetime.strptime(d, '%Y.%m.%d')
    d = local_to_utc(d, 'Europe/Budapest')
    return d.strftime(DATETIME_FORMAT)


def pl_to_datetime(d):
    if d == '0000-00-00':
        return None
    try:
        d = datetime.strptime(d, '%Y-%m-%d %H:%M:%S')
        d = local_to_utc(d, 'Europe/Warsaw')
    except ValueError:
        d = datetime.strptime(d, '%Y-%m-%d')
    return d


def pl_to_iso(d):
    dt = pl_to_datetime(d)
    if dt is None:
        return None
    return dt.date().isoformat()


def pl_to_iso_datetime(d):
    dt = pl_to_datetime(d)
    if dt is None:
        return None
    return dt.strftime(DATETIME_FORMAT)


def pl_make_session_id(value):
    return 'sejm_posiedzenia/%s' % value


def pl_make_sitting_id(value):
    return 'sejm_posiedzenia_punkty/%s' % value


class PersonLoader(ItemLoader):
    default_input_processor = MapCompose(strip)
    default_output_processor = TakeFirst()


class SkupstinaMePersonLoader(PersonLoader):
    birth_date_in = MapCompose(
        PersonLoader.default_input_processor,
        me_to_iso
    )

    image_in = MapCompose(
        PersonLoader.default_input_processor,
        lambda i: "http://www.skupstina.me%s" % i
    )


class MojePanstwoPersonLoader(PersonLoader):
    birth_date_in = MapCompose(
        PersonLoader.default_input_processor,
        pl_to_iso
    )


class OrganizationLoader(ItemLoader):
    default_input_processor = MapCompose(strip)
    default_output_processor = TakeFirst()

    other_names_out = MakeList()


class ParlamentHuOrganizationLoader(OrganizationLoader):
    founding_date_in = MapCompose(hu_to_iso)
    dissolution_date_in = MapCompose(hu_to_iso)


class MembershipLoader(ItemLoader):
    default_input_processor = MapCompose(strip)
    default_output_processor = TakeFirst()


class MojePanstwoMembershipLoader(MembershipLoader):
    start_date_in = MapCompose(pl_to_iso)
    end_date_in = MapCompose(pl_to_iso)


class ParlamentHuMembershipLoader(MembershipLoader):
    ROLES = {
        u'frakciótitkár': u'secretary of the parliamentary group',
        u'Elnök': u'president',
        u'frakcióvez. h.': u'deputy leader of parliamentary group',
        u'Titkár': u'secretary',
        u'frakcióvezető': u'leader of parliamentary group',
        u'Alelnök': u'vice president',
        u'Társelnök': u'co-president',
        u'Tag': u'member',
        u'Helyettesítő alelnök': u'substituting vice president'
    }
    start_date_in = MapCompose(hu_to_iso)
    end_date_in = MapCompose(hu_to_iso)
    role_in = MapCompose(lambda x: translate(
        x, ParlamentHuMembershipLoader.ROLES))


class MotionLoader(ItemLoader):
    default_output_processor = TakeFirst()


class ParlamentHuMotionLoader(MotionLoader):
    REQUIREMENT_OPTIONS = {
        u'Jelenlét megállapítás': u'to determine attendance',
        u'Listás': u'ballot',
        u'Listás a jelenlevők 2/3-ával': u'ballot with 2/3 of present mps',
        u'Listás a jelenlevők 4/5-ével': u'ballot with 4/5 of present mps',
        u'Listás az összes képviselő felével': u'ballot with 1/2 of all mps',
        u'Listás az összes képviselő 2/3-ával': u'ballot with 2/3 of all mps',
        u'Listás az összes képviselő 4/5-ével': u'ballot with 4/5 of all mpsl',
        u'Név nélküli': u'anonymus',
        u'Név nélküli a jelenlevők 2/3-ával': u'anonymous with 2/3 of present mps',
        u'Név nélküli a jelenlevők 4/5-ével': u'anonymous with 1/2 of present mps',
        u'Név nélküli az összes képviselő felével': u'anonymous with 1/2 of all mps',
        u'Név nélküli az összes képviselő 2/3-ával': u'anonymous with 2/3 of all mps',
        u'Név nélküli az összes képviselő 4/5-ével': u'anonymous with 4/5 of all mps',
        u'Név nélküli jelenlét megállapítás': u'anonymous to determine attendance',
        u'Név szerinti': u'roll-call',
        u'Név szerinti a jelenlevők 2/3-ával': u'roll-call with 2/3 of present mps',
        u'Név szerinti az összes képviselő felével': u'roll-call with 1/2 of all mps',
        u'Név szerinti az összes képviselő 2/3-ával': u'roll-call with 2/3 of all mps',
        u'Titkos': u'secret',
        u'Titkos az összes képviselő 2/3-ával': u'secret with 2/3 of all mps',
    }

    requirement_in = MapCompose(lambda x: translate(
        x, ParlamentHuMotionLoader.REQUIREMENT_OPTIONS))


class SkupstinaMeMotionLoader(MotionLoader):
    RESULT_OPTIONS = {
        'usvojen': 'pass',
        'nije usvojen': 'fail'
    }

    result_in = MapCompose(lambda x: translate(
        x, SkupstinaMeMotionLoader.RESULT_OPTIONS, allow_empty=True))
    date_in = MapCompose(me_to_iso_datetime)


class MojePanstwoMotionLoader(MotionLoader):
    VOTING_RESULTS = {
        '1': 'pass',
        '2': 'fail',
    }

    date_in = MapCompose(pl_to_iso_datetime)
    result_in = MapCompose(lambda x: translate(
        x, MojePanstwoMotionLoader.VOTING_RESULTS))
    legislative_session_id_in = MapCompose(strip, pl_make_session_id)


class CountLoader(ItemLoader):
    default_output_processor = TakeFirst()

    value = MapCompose(int)


class VoteLoader(ItemLoader):
    default_output_processor = TakeFirst()


class ParlamentHuVoteLoader(VoteLoader):
    VOTE_OPTIONS = {
        u'Igen': 'yes',
        u'Nem': 'no',
        u'Tart.': 'abstain',
        u'Ig.távol': 'excused',
        u'Nem szav.': 'absent',
        u'Jelen, nem szav.': 'not voting'
    }

    option_in = MapCompose(
        lambda x: translate(x, ParlamentHuVoteLoader.VOTE_OPTIONS))


class MojePanstwoVoteLoader(VoteLoader):
    VOTE_OPTIONS = {
        '1': 'yes',
        '2': 'no',
        '3': 'abstain',
        '4': 'absent',
    }

    option_in = MapCompose(
        lambda x: translate(x, MojePanstwoVoteLoader.VOTE_OPTIONS))


class VoteEventLoader(ItemLoader):
    default_output_processor = TakeFirst()


class ParlamentHuVoteEventLoader(VoteEventLoader):
    VOTING_RESULTS = {
        u'Elfogadott': 'pass',
        u'Elvetett': 'fail',
        u'Határozatképes': 'pass',  # quorum
        u'Határozatképtelen': 'fail'
    }

    start_date_in = MapCompose(hu_to_iso_datetime)
    result_in = MapCompose(lambda x: translate(
        x, ParlamentHuVoteEventLoader.VOTING_RESULTS))


class MojePanstwoVoteEventLoader(VoteEventLoader):
    VOTING_RESULTS = {
        '1': 'pass',
        '2': 'fail',
    }

    start_date_in = MapCompose(pl_to_iso_datetime)
    result_in = MapCompose(lambda x: translate(
        x, MojePanstwoVoteEventLoader.VOTING_RESULTS))


class SpeechLoader(ItemLoader):
    default_input_processor = MapCompose(strip)
    default_output_processor = TakeFirst()


def normalize_position_hu(pos):
    pos = pos.split('-')[0]
    if pos.isdigit():
        return int(pos)


class ParlamentHuSpeechLoader(SpeechLoader):
    date_in = MapCompose(hu_to_iso_datetime)
    position_in = MapCompose(normalize_position_hu)


class MojePanstwoSpeechLoader(SpeechLoader):
    date_in = MapCompose(pl_to_iso_datetime)
    event_id_in = MapCompose(strip, pl_make_sitting_id)


class SkupstinaMeSpeechLoader(SpeechLoader):
    pass


class EventLoader(ItemLoader):
    default_input_processor = MapCompose(strip)
    default_output_processor = TakeFirst()


class MojePanstwoEventLoader(EventLoader):
    start_date_in = MapCompose(pl_to_iso_datetime)
    end_date_in = MapCompose(pl_to_iso_datetime)


class MojePanstwoSessionLoader(MojePanstwoEventLoader):
    identifier_in = MapCompose(strip, pl_make_session_id)


class MojePanstwoSittingLoader(MojePanstwoEventLoader):
    identifier_in = MapCompose(strip, pl_make_sitting_id)
    parent_id_in = MapCompose(strip, pl_make_session_id)


def join_text(value):
    stripped = map(strip, value)
    return ' '.join(filter(len, stripped))


class ParlamentHuEventLoader(EventLoader):
    name_in = Compose(join_text)
    start_date_in = MapCompose(hu_to_iso_datetime)


class SkupstinaMeEventLoader(EventLoader):
    start_date_in = MapCompose(me_start_date)
    end_date_in = MapCompose(me_end_date)
