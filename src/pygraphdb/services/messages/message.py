__author__ = 'Sagar'

class Message(object):
    def __str__(self):
        return "%s:%d" % (self.__class__.__name__, id(self))