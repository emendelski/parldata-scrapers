def parse_identifier(identifier, loader_context):
    r = {'identifier': identifier}
    if 'scheme' in loader_context:
        r['scheme'] = loader_context['scheme']
    return r


def parse_other_names(value, loader_context):
    return {'name': value}


class MakeList(object):
    def __call__(self, values):
        if type(values) != list:
            values = [values]
        return values
