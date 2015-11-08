__author__ = 'Sagar'
from pickle import dumps
from pygraphdb.services.worker.dummynode import DummyNode
class IndexBlock(object):
    def __init__(self, location, index_name, index_directory):
        super(IndexBlock, self).__init__()
        self._location = location
        self._index_name = index_name
        self._index_directory = index_directory
        self._index_nodes = []
        self._parent_block = -1
        self._max_size = 2048
        self._size = 0
        self._dummy_node = None

    def create_dummy_node(self):
        self._dummy_node = DummyNode()

    #def update_child_blocks(self, fd):

    def is_leaf(self):
        for node in self._index_nodes:
            if node.get_index_block_pointer() != -1:
                return False
        if self._dummy_node.get_index_block_pointer() != 1:
            return False
        return True

    def delete_index_node(self, index_node):
        i = 0
        while i < len(self._index_nodes):
            if self._index_nodes[i].get_key() == index_node.get_key() and self._index_nodes[i].get_value() == index_node.get_value():
                self._index_nodes.pop(i)
                break
            i += 1

    def add_index_node(self, index_node):
        self._index_nodes.append(index_node)
        self._index_nodes = sorted(self._index_nodes, key = lambda index_node: index_node.get_key())

    def get_location(self):
        return self._location

    def set_location(self, location):
        self._location = location

    def get_index_nodes(self):
        return self._index_nodes

    def get_parent_block(self):
        return self._parent_block

    def set_parent_block(self, parent):
        self._parent_block = parent

    def get_dummy_node_pointer(self):
        return self._dummy_node.get_index_block_pointer()

    def get_size(self):
        data = dumps(self)
        return len(data) + 4

    def set_dummy_node_pointer(self, pointer):
        self._dummy_node.set_index_block_pointer(pointer)