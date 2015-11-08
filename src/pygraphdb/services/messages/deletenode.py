__author__ = 'Sagar'
from pygraphdb.services.messages.message import Message

class DeleteNode(Message):
    def __init__(self, node, index=None):
        super(DeleteNode, self).__init__()
        self._node = node
        self._index = index

    def get_node(self):
        return self._node

    def get_index(self):
        return self._index
