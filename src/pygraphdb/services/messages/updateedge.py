__author__ = 'Sagar'
from pygraphdb.services.messages.message import Message

class UpdateEdge(Message):
    def __init__(self, edge, index=None):
        super(UpdateEdge, self).__init__()
        self._edge = edge
        self._index = index

    def get_edge(self):
        return self._edge

    def get_index(self):
        return self._index