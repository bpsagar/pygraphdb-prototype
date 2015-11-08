__author__ = 'Sagar'
from pygraphdb.services.messages.message import Message

class DeleteType(Message):
    def __init__(self, type):
        self._type = type

    def get_type(self):
        return self._type