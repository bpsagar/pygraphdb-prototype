__author__ = 'Sagar'
from pygraphdb.services.messages.message import Message

class FindNode(Message):
    def __init__(self, key, type):
        super(FindNode, self).__init__()
        self._key = key
        self._type = type

    def get_key(self):
        return self._key

    def get_type(self):
        return self._type
