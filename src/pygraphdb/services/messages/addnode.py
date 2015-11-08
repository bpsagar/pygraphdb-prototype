__author__ = 'Sagar'
from pygraphdb.services.messages.message import Message

class AddNode(Message):
    def __init__(self, node, batch=False):
        super(AddNode, self).__init__()
        self._node = node
        self._batch = batch

    def get_node(self):
        return self._node

    def get_batch(self):
        return self._batch