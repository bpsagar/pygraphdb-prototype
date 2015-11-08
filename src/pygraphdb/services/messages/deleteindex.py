__author__ = 'Sagar'

class DeleteIndex(object):
    def __init__(self, index_name, index_node, type):
        super(DeleteIndex, self).__init__()
        self._index_name = index_name
        self._index_node = index_node
        self._type = type

    def get_index_name(self):
        return self._index_name

    def get_index_node(self):
        return self._index_node

    def get_type(self):
        return self._type
