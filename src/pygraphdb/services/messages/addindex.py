__author__ = 'Sagar'
from pygraphdb.services.messages.message import Message

class AddIndex(Message):
    def __init__(self, index_name, index_node, type):
        super(AddIndex, self).__init__()
        self._index_name = index_name
        self._index_node = index_node
        self._type = type

    def get_index_name(self):
        return self._index_name

    def get_index_node(self):
        return self._index_node

    def get_type(self):
        return self._type