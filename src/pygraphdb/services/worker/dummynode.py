__author__ = 'Sagar'

class DummyNode(object):
    def __init__(self):
        self._index_block_pointer = -1

    def set_index_block_pointer(self, pointer):
        self._index_block_pointer = pointer

    def get_index_block_pointer(self):
        return self._index_block_pointer