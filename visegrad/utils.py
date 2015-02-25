import itertools

import re


def parse_identifier(identifier, loader_context):
    r = {'identifier': identifier}
    if 'scheme' in loader_context:
        r['scheme'] = loader_context['scheme']
    return r


def parse_hu_name(value):
    r = dict(
        given_name='', family_name='', additional_name='', honorific_prefix='')

    pattern = re.compile(r'^(dr\.\s?)', re.I | re.U)
    match = pattern.match(value)
    if match:
        r['honorific_prefix'] = ''.join(match.groups()).strip()
        value = pattern.sub('', value)

    pattern = re.compile(r'^((?:\w\.\s)?[\w-]+)\s(\w+)$', re.U)
    match = pattern.match(value)
    if match:
        r['family_name'], r['given_name'] = match.groups()

    return r


def parse_other_names(value, loader_context):
    return {'name': value}


def chunks(iterator, size=50, filter_func=None):
    filtered_iterator = iterator
    if filter_func:
        filtered_iterator = itertools.ifilter(filter_func, iterator)
    filtered_iterator = iter(filtered_iterator)
    chunk = list(itertools.islice(filtered_iterator, size))
    while chunk:
        yield chunk
        chunk = list(itertools.islice(filtered_iterator, size))


class MakeList(object):
    def __call__(self, values):
        if type(values) != list:
            values = [values]
        return values
