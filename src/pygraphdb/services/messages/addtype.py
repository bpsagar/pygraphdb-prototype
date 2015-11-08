__author__ = 'Sagar'
from pygraphdb.services.messages.message import Message

class AddType(Message):
    def __init__(self, type):
        super(AddType, self).__init__()
        self._type = type

    def get_type(self):
        return self._type