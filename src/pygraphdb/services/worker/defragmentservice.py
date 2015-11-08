__author__ = 'Sagar'
from pygraphdb.services.common.service import Service
from queue import Queue, Empty
import os
class DefragmentIndex(object):
    def __init__(self, index_name, index_directory):
        super(DefragmentIndex, self).__init__()
        self._index_name = index_name
        self._index_directory = index_directory

    def get_index_name(self):
        return self._index_name

    def get_index_directory(self):
        return self._index_directory

class DefragmentService(Service):
    def construct(self, config={}):
        self._communication_service = config.get('communication_service', None)
        self._queue = Queue()

    def init(self):
        pass

    def deinit(self):
        pass

    def do_work(self):
        try:
            source_name, source_service, message = self._queue.get(True, self._timeout)
        except Empty:
            raise TimeoutError

        if isinstance(message, DefragmentIndex):
            self.defragment(message)

    def defragment(self, message):
        index_name = message.get_index_name()
        index_directory = message.get_index_directory()
        file = os.path.join(index_directory, index_name)
        self._fd = open(file, 'rb+')


