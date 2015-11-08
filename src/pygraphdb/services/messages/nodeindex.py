__author__ = 'Sagar'
from pygraphdb.services.messages.message import Message
class NodeIndex(Message):
    def __init__(self, node, location):
        self._node = node
        self._location = location

    def get_node(self):
        return self._node

    def get_location(self):
        return self._location
