# Define here the scrapy.Field()
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.item import Item
from scrapy.contrib.loader.processor import MapCompose

from urlparse import urljoin

from visegrad.serializers import IdentifiersSerializer
from visegrad.utils import parse_identifier, parse_other_names, MakeList


class Dateframeable(Item):
    start_date = scrapy.Field()
    end_date = scrapy.Field()


def get_full_url(url, loader_context):
    if url.startswith('http://') or url.startswith('https://'):
        return url

    response_url = loader_context['response_url']
    return urljoin(response_url, url)


class Person(Item):
    name = scrapy.Field()
    other_names = scrapy.Field(
        input_processor=MapCompose(parse_other_names),
        output_processor=MakeList()
    )
    identifiers = scrapy.Field(
        input_processor=MapCompose(parse_identifier),
        output_processor=MakeList()
    )
    family_name = scrapy.Field()
    given_name = scrapy.Field()
    additional_name = scrapy.Field()
    honorific_prefix = scrapy.Field()
    honorific_suffix = scrapy.Field()
    patronymic_name = scrapy.Field()
    sort_name = scrapy.Field()
    email = scrapy.Field()
    gender = scrapy.Field()
    birth_date = scrapy.Field()
    death_date = scrapy.Field()
    summary = scrapy.Field()
    biography = scrapy.Field()
    image = scrapy.Field(
        input_processor=MapCompose(get_full_url),
    )
    links = scrapy.Field(
        input_processor=MapCompose(lambda x: {'url': x}),
        output_processor=MakeList()
    )
    sources = scrapy.Field(
        input_processor=MapCompose(lambda x: {'url': x}),
        output_processor=MakeList()
    )


class SkupstinaMePerson(Person):
    identifiers = scrapy.Field(Person.fields['identifiers'],
        serializer=IdentifiersSerializer('skupstina.me/people'))


class Organization(Item):
    name = scrapy.Field()
    other_names = scrapy.Field(
        input_processor=MapCompose(parse_other_names),
        output_processor=MakeList()
    )
    identifiers = scrapy.Field(
        input_processor=MapCompose(parse_identifier),
        output_processor=MakeList()
    )
    classification = scrapy.Field()
    parent = scrapy.Field()
    dissolution_date = scrapy.Field()
    founding_date = scrapy.Field()
    contact_details = scrapy.Field()
    start_date = scrapy.Field()
    end_date = scrapy.Field()
    sources = scrapy.Field(
        input_processor=MapCompose(lambda x: {'url': x}),
        output_processor=MakeList()
    )


class Membership(Item):
    label = scrapy.Field()
    role = scrapy.Field()
    member = scrapy.Field()
    person_id = scrapy.Field()
    organization_id = scrapy.Field()
    post_id = scrapy.Field()
    on_behalf_of_id = scrapy.Field()
    area_id = scrapy.Field()
    start_date = scrapy.Field()
    end_date = scrapy.Field()
    contact_details = scrapy.Field()
    sources = scrapy.Field(
        input_processor=MapCompose(lambda x: {'url': x}),
        output_processor=MakeList()
    )


class Motion(Item):
    id = scrapy.Field()
    organization_id = scrapy.Field()
    legislative_session = scrapy.Field()
    legislative_session_id = scrapy.Field()
    creator_id = scrapy.Field()
    text = scrapy.Field()
    classification = scrapy.Field()
    date = scrapy.Field()
    requirement = scrapy.Field()
    result = scrapy.Field()
    sources = scrapy.Field(
        input_processor=MapCompose(lambda x: {'url': x}),
        output_processor=MakeList()
    )


class Count(Item):
    option = scrapy.Field()
    value = scrapy.Field()


class VoteEvent(Item):
    identifier = scrapy.Field()
    motion_id = scrapy.Field()
    motion = scrapy.Field()
    organization_id = scrapy.Field()
    organization = scrapy.Field()
    legislative_session_id = scrapy.Field()
    legislative_session = scrapy.Field()
    result = scrapy.Field()
    group_results = scrapy.Field()
    counts = scrapy.Field(output_processor=MakeList())
    start_date = scrapy.Field()
    end_date = scrapy.Field()
    sources = scrapy.Field(
        input_processor=MapCompose(lambda x: {'url': x}),
        output_processor=MakeList()
    )


class Vote(Item):
    option = scrapy.Field()
    vote_event_id = scrapy.Field()
    pair_id = scrapy.Field()
    group_id = scrapy.Field()
    voter_id = scrapy.Field(
        input_processor=MapCompose(parse_identifier)
    )
    role = scrapy.Field()
    weight = scrapy.Field()


class Speech(Item):
    creator_id = scrapy.Field(
        input_processor=MapCompose(parse_identifier)
    )
    role = scrapy.Field()
    attribution_text = scrapy.Field()
    audience_id = scrapy.Field()
    text = scrapy.Field()
    audio = scrapy.Field()
    video = scrapy.Field()
    date = scrapy.Field()
    title = scrapy.Field()
    type = scrapy.Field()
    position = scrapy.Field()
    event_id = scrapy.Field()
    sources = scrapy.Field(
        input_processor=MapCompose(lambda x: {'url': x}),
        output_processor=MakeList()
    )


class Event(Item):
    name = scrapy.Field()
    identifier = scrapy.Field()
    organization_id = scrapy.Field()
    type = scrapy.Field()
    start_date = scrapy.Field()
    end_date = scrapy.Field()
    sources = scrapy.Field(
        input_processor=MapCompose(lambda x: {'url': x}),
        output_processor=MakeList()
    )
