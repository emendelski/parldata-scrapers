class IdentifiersSerializer(object):
    def __init__(self, scheme):
        self.scheme = scheme

    def __call__(self, value):
        return [{'identifier': value, 'scheme': self.scheme}]
