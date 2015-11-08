__author__ = 'Sagar'
from pygraphdb.services.messages.message import Message

class AddEdge(Message):
    def __init__(self, edge):
        super(AddEdge, self).__init__()
        self._edge = edge

    def get_edge(self):
        return self._edge

