__author__ = 'Sagar'
import json

class Edge(object):
    def __init__(self, id, node1_id=None, node2_id=None, type=None):
        super(Edge, self).__init__()
        self._id = id
        self._node1_id = node1_id
        self._node2_id = node2_id
        self._type = type
        self._properties = {}

    def dump(self):
        export = {}
        export['id'] = self._id
        export['type'] = self._type
        export['node1_id'] = self._node1_id
        export['node2_id'] = self._node2_id
        export['properties'] = self._properties
        return json.dumps(export)

    def load(self, data):
        load_data = json.loads(data)
        self._id = load_data['id']
        self._type = load_data['type']
        self._node1_id = load_data['node1_id']
        self._node2_id = load_data['node2_id']
        self._properties = load_data['properties']

    def set_property(self, name, value):
        self._properties[name] = value

    def get_property(self, name):
        if name in self._properties.keys():
            return self._properties[name]
        return None

    def get_properties(self):
        return self._properties

    def get_id(self):
        return self._id

    def get_type(self):
        return self._type

    def get_node1_id(self):
        return self._node1_id

    def get_node2_id(self):
        return self._node2_id