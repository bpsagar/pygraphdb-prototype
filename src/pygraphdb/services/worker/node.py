__author__ = 'Sagar'
import json

class Node(object):
    def __init__(self, id=None, type=None, properties={}):
        self._id = id
        self._type = type
        self._properties = properties

    def dump(self):
        export = {}
        export['id'] = self._id
        export['type'] = self._type
        export['properties'] = self._properties
        return json.dumps(export)

    def load(self, data):
        load_data = json.loads(data)
        self._id = load_data['id']
        self._type = load_data['type']
        self._properties = load_data['properties']

    def set_property(self, name, value):
        self._properties[name] = value

    def get_property(self, name):
        if name in self._properties.keys():
            return self._properties[name]
        return None

    def get_id(self):
        return self._id

    def get_type(self):
        return self._type

    def get_properties(self):
        return self._properties