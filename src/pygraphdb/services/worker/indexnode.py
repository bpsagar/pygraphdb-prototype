__author__ = 'Sagar'

class IndexNode(object):
    def __init__(self, key, value, id=None, pointer=-1):
        super(IndexNode, self).__init__()
        self._key = key
        self._value = value
        self._id = id
        self._index_block_pointer = pointer

    def get_key(self):
        return self._key

    def get_value(self):
        return self._value

    def get_index_block_pointer(self):
        return self._index_block_pointer

    def get_size(self):
        size = 4 + len(self._key) + 4 + 4
        # 4 - length of the key, 4 - value, 4 - block pointer
        return size

    def set_index_block_pointer(self, pointer):
        self._index_block_pointer = pointer

    def get_id(self):
        return self._id
