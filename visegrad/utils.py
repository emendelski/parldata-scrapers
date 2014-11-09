import itertools


def parse_identifier(identifier, loader_context):
    r = {'identifier': identifier}
    if 'scheme' in loader_context:
        r['scheme'] = loader_context['scheme']
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
