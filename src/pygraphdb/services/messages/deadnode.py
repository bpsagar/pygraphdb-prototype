__author__ = 'Sagar'
from pygraphdb.services.messages.message import Message

class DeadNode(Message):
    def __init__(self, client):
        super(DeadNode, self).__init__()
        self._client = client

    def get_client(self):
        return self._client
