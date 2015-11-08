__author__ = 'Sagar'
from pygraphdb.services.messages.message import Message

class FindIndex(Message):
    def __init__(self, index_name, key, type):
        super(FindIndex, self).__init__()
        self._index_name = index_name
        self._key = key
        self._type = type

    def get_index_name(self):
        return self._index_name

    def get_key(self):
        return self._key

    def get_type(self):
        return self._type