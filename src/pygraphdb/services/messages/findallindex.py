__author__ = 'Sagar'
from pygraphdb.services.messages.message import Message

class Operator(object):
    equal_to = 1
    greater_than = 2
    lesser_than = 3
    greater_than_or_equal_to = 4
    lesser_than_or_equal_to = 5

    def get_operator(string):
        dict = {}
        dict['=='] = Operator.equal_to
        dict['>'] = Operator.greater_than
        dict['<'] = Operator.lesser_than
        dict['>='] = Operator.greater_than_or_equal_to
        dict['<='] = Operator.lesser_than_or_equal_to
        return dict[string]

    def get_opposite(string):
        op = Operator.get_operator(string)
        if op == Operator.equal_to:
            return op
        if op == Operator.greater_than:
            return Operator.lesser_than
        if op == Operator.lesser_than:
            return Operator.greater_than
        if op == Operator.greater_than_or_equal_to:
            return Operator.lesser_than_or_equal_to
        if op == Operator.lesser_than_or_equal_to:
            return Operator.greater_than_or_equal_to

class FindAllIndex(Message):
    def __init__(self, index_name, key, type, operator, ret):
        super(FindAllIndex, self).__init__()
        self._key = key
        self._operator = operator
        self._index_name = index_name
        self._type = type
        self._return = ret

    def get_return(self):
        return self._return

    def get_index_name(self):
        return self._index_name

    def get_operator(self):
        return self._operator

    def get_key(self):
        return self._key

    def get_type(self):
        return self._type





