from collections import namedtuple

Dependency = namedtuple('Dependency', ['model', 'ids'])

EntityObject = namedtuple('EntityObject', ['model', 'preloaded', 'ids', 'serializer'])

ApiObject = namedtuple('ApiObject', ['result', 'entities'])

ApiResult = namedtuple('ApiResult', ['objects', 'type'])
