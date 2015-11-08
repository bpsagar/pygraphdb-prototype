__author__ = 'Sagar'
import json

class Type():
    def __init__(self, type=None, parent_type=None, description=None):
        super(Type, self).__init__()
        self._type = type
        self._parent_type = parent_type
        self._description = description

    def get_type(self):
        return self._type

    def get_parent_type(self):
        return self._parent_type

    def get_description(self):
        return self._description

    def set_type(self, type):
        self._type = type

    def set_parent_type(self, parent):
        self._parent_type = parent

    def set_description(self, description):
        self._description = description

    def dump(self):
        export = {}
        export['type'] = self._type
        export['parent_type'] = self._parent_type
        export['description'] = self._description
        return json.dumps(export)

    def load(self, data):
        load_data = json.loads(data)
        self._type = load_data['type']
        self._parent_type = load_data['parent_type']
        self._description = load_data['description']

