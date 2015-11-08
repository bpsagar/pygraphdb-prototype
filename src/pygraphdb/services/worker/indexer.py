__author__ = 'Sagar'
import os
from struct import pack, unpack
from pygraphdb.services.worker.indexblock import IndexBlock
from pygraphdb.services.messages.findallindex import Operator
from pickle import loads, dumps

class Indexer(object):
    def __init__(self, index_name, index_directory, type, block_size):
        super(Indexer, self).__init__()
        self._index_name = index_name
        self._index_directory = index_directory
        self._block_size = block_size
        self._type = type
        file_name = self._type + "-" + self._index_name
        file = os.path.join(self._index_directory, file_name)
        # create a file if doesnt exist
        open(file, 'a').close()
        self._fd = open(file, 'rb+')

    def stop(self):
        self._fd.close()

    # Basic functions.

    def read_integer_field(self):
        try:
            packed_data = self._fd.read(8)
            value = unpack('l', packed_data)
        except Exception as e:
            print(packed_data)
            raise e
        return value[0]

    def write_integer_field(self, data):
        packed_data = pack('l', data)
        self._fd.write(packed_data)
        self._fd.flush()

    def get_free_block_pointer(self):
        self._fd.seek(8, 0)
        free_block = self.read_integer_field()
        if free_block == -1:
            self._fd.seek(0, 2)
            free_block = self._fd.tell()
            for i in range(self._block_size):
                self._fd.write(bytes(' ', 'UTF-8'))
                self._fd.flush()
        else:
            self._fd.seek(free_block+1, 0)
            next_free_block = self.read_integer_field()
            self.set_free_block_pointer(next_free_block)
        return free_block

    def set_free_block_pointer(self, free_block):
        self._fd.seek(8, 0)
        self.write_integer_field(free_block)

    def get_root_block(self):
        self._fd.seek(0, 0)
        packed_data = self._fd.read(8)
        if packed_data == b'':
            self.set_root_block(-1)
            self.set_free_block_pointer(-1)
            return -1
        else:
            root_block = unpack('l', packed_data)
        return root_block[0]

    def set_root_block(self, root):
        self._fd.seek(0, 0)
        self.write_integer_field(root)

    def read_block(self, block_location):
        self._fd.seek(block_location, 0)
        size = self.read_integer_field()
        fsize = size
        full_data = b''
        while size > 0:
            data = self._fd.read(size)
            full_data += data
            size -= len(data)
        try:
            block = loads(full_data)
        except:
            raise EOFError
        return block

    def write_block(self, block):
        self._fd.seek(block.get_location(), 0)
        if block.get_location() == 20488:
            x = 1
        data = dumps(block)
        size = len(data)
        self.write_integer_field(size)
        remaining = self._block_size - size - 8
        if remaining < 0:
            print("Block size exceeded.")
            print(block.get_location(), size)
        while remaining > 0:
            data += b"\0"
            remaining -= 1
        self._fd.write(data)
        self._fd.flush()

    def delete_block(self, block):
        self._fd.seek(8, 0)
        free_block_pointer = self.read_integer_field()
        self._fd.seek(block.get_location(), 0)
        self._fd.write(bytes('*', 'UTF-8'))
        self.write_integer_field(free_block_pointer)
        remaining = self._block_size - 5
        data = b''
        for i in range(remaining):
            data += b'\0'

        self._fd.write(data)
        self._fd.flush()
        self.set_free_block_pointer(block.get_location())

    def update_child_blocks(self, block):
        for node in block.get_index_nodes():
            child_block_location = node.get_index_block_pointer()
            if child_block_location != -1:
                child_block = self.read_block(child_block_location)
                child_block.set_parent_block(block.get_location())
                self.write_block(child_block)
        child_block_location = block.get_dummy_node_pointer()
        if child_block_location != -1:
            child_block = self.read_block(child_block_location)
            child_block.set_parent_block(block.get_location())
            self.write_block(child_block)

    # Functions to find an index in the index tree.

    def find_index_node(self, key):
        block_location = self.get_root_block()
        while block_location != -1:
            block = self.read_block(block_location)
            flag = 0
            for node in block.get_index_nodes():
                if key < node.get_key():
                    flag = 1
                    block_location = node.get_index_block_pointer()
                    break
                elif key == node.get_key():
                    return node.get_value()
            if flag == 0:
                block_location = block.get_dummy_node_pointer()

        return None

    # Functions to add a new index key to the index tree.

    def find_leaf_block(self, key, root):
        if root == -1:
            block = IndexBlock(self.get_free_block_pointer(), self._index_name, self._index_directory)
            block.create_dummy_node()
            return block

        block = self.read_block(root)
        parent = -1

        while not block.is_leaf():
            parent = block.get_location()
            index_nodes = block.get_index_nodes()
            flag = 0
            for node in index_nodes:
                if key < node.get_key():
                    flag = 1
                    block_location = node.get_index_block_pointer()
                    break
            if flag == 0:
                block_location = block.get_dummy_node_pointer()
            if block_location == -1:
                return self.read_block(parent)
            if block_location == None:
                x = 1
            block = self.read_block(block_location)
        return block

    def split_block(self, block):
        index_nodes = block.get_index_nodes()
        median = int(len(index_nodes)/2)
        median_node = index_nodes[median]

        new_block = IndexBlock(self.get_free_block_pointer(), self._index_name, self._index_directory)
        new_block.create_dummy_node()
        i = 0
        while i < median:
            new_block.add_index_node(index_nodes[i])
            i += 1

        new_block.set_dummy_node_pointer(median_node.get_index_block_pointer())
        i = 0
        while i <= median:
            block.delete_index_node(index_nodes[0])
            i += 1

        self.update_child_blocks(new_block)
        median_node.set_index_block_pointer(new_block.get_location())
        new_block.set_parent_block(block.get_parent_block())
        return new_block, median_node

    def add_index(self, index_node, block=None):
        key = index_node.get_key()
        root_block_location = self.get_root_block()

        if block is None:
            block = self.find_leaf_block(key, root_block_location)

        parent = block.get_parent_block()
        block.add_index_node(index_node)

        if block.get_size() < (0.80*self._block_size):
            self.write_block(block)
            if root_block_location == -1:
                self.set_root_block(block.get_location())
            return

        new_block, median_node = self.split_block(block)
        if parent != -1:
            parent = self.read_block(parent)
            self.write_block(block)
            self.write_block(new_block)
            self.add_index(median_node, parent)
        else:
            root_block = IndexBlock(self.get_free_block_pointer(), self._index_name, self._index_directory)
            root_block.create_dummy_node()
            root_block.set_dummy_node_pointer(block.get_location())
            root_block.add_index_node(median_node)
            block.set_parent_block(root_block.get_location())
            new_block.set_parent_block(root_block.get_location())
            self.set_root_block(root_block.get_location())
            self.write_block(root_block)
            self.write_block(new_block)
            self.write_block(block)

    # Functions to delete an index from the index tree.

    def find_index_block(self, index_node):
        block_location = self.get_root_block()
        while block_location != -1:
            block = self.read_block(block_location)
            flag = 0
            for node in block.get_index_nodes():
                if index_node.get_key() < node.get_key():
                    flag = 1
                    block_location = node.get_index_block_pointer()
                    break
                elif index_node.get_key() == node.get_key():
                    return block, node
            if flag == 0:
                block_location = block.get_dummy_node_pointer()
        return None, None

    def find_successor(self, block, index_node):
        index_nodes = block.get_index_nodes()

        for i in range(len(index_nodes)):
            if index_nodes[i].get_key() == index_node.get_key():
                break
        i += 1
        if i >= len(index_nodes):
            block_location = block.get_dummy_node_pointer()
        else:
            block_location = index_nodes[i].get_index_block_pointer()

        if block_location == -1:
            if i >= len(index_nodes):
                return block, None
            else :
                return block, index_nodes[i]

        block = self.read_block(block_location)

        if len(block.get_index_nodes()) > 0:
            block_location = block.get_index_nodes()[0].get_index_block_pointer()
        else:
            block_location = block.get_dummy_node_pointer()
        while block_location != -1:
            block = self.read_block(block_location)
            if len(block.get_index_nodes()) > 0:
                block_location = block.get_index_nodes()[0].get_index_block_pointer()
            else:
                block_location = block.get_dummy_node_pointer()
        if len(block.get_index_nodes()) > 0:
            return block, block.get_index_nodes()[0]
        else:
            return block, None

    def delete_index_node(self, index_node):
        block, index_node = self.find_index_block(index_node)
        if block is None:
            print("Fail")
            return

        successor_block, successor_node = self.find_successor(block, index_node)

        if block == successor_block:
            if successor_node is None:
                block.set_dummy_node_pointer(index_node.get_index_block_pointer())
            else:
                successor_node.set_index_block_pointer(index_node.get_index_block_pointer())
            block.delete_index_node(index_node)
            if len(block.get_index_nodes()) == 0:
                if block.get_location() != self.get_root_block():
                    parent_location = block.get_parent_block()
                    if parent_location != -1:
                        parent = self.read_block(parent_location)

                        flag = 0
                        for node in parent.get_index_nodes():
                            if node.get_index_block_pointer() == block.get_location():
                                flag = 1
                                node.set_index_block_pointer(block.get_dummy_node_pointer())
                        if flag == 0:
                            parent.set_dummy_node_pointer(block.get_dummy_node_pointer())
                        self.update_child_blocks(parent)
                        self.write_block(parent)
                        self.delete_block(block)
                else:
                    self.write_block(block)
            else:
                self.write_block(block)
        else :
            successor_block.delete_index_node(successor_node)
            block.delete_index_node(index_node)
            successor_node.set_index_block_pointer(index_node.get_index_block_pointer())
            block.add_index_node(successor_node)
            self.write_block(block)
            if len(successor_block.get_index_nodes()) == 0:
                if successor_block.get_location() != self.get_root_block():
                    parent_location = successor_block.get_parent_block()
                    if parent_location != -1:
                        parent = self.read_block(parent_location)
                        flag = 0
                        for node in parent.get_index_nodes():
                            if node.get_index_block_pointer() == successor_block.get_location():
                                flag = 1
                                node.set_index_block_pointer(successor_block.get_dummy_node_pointer())
                        if flag == 0:
                            parent.set_dummy_node_pointer(successor_block.get_dummy_node_pointer())
                        self.update_child_blocks(parent)
                        self.write_block(parent)
                    self.delete_block(successor_block)
                else:
                    self.write_block(successor_block)
            else:
                self.write_block(successor_block)

    # Finding all indexes based on the operator
    def traverse_index_tree(self, block_location, key, operator, search_blocks, results):
        if block_location == -1:
            return
        block = self.read_block(block_location)
        index_nodes = block.get_index_nodes()

        for node in index_nodes:

            # for < operator
            if operator == Operator.lesser_than:
                if node.get_key() > key:
                    search_blocks.append(node.get_index_block_pointer())
                    break
                elif node.get_key() < key:
                    search_blocks.append(node.get_index_block_pointer())
                    results.append(node)
                elif key == node.get_key():
                    search_blocks.append(node.get_index_block_pointer())

            #for > operator
            if operator == Operator.greater_than:
                if node.get_key() < key:
                    continue
                elif node.get_key() > key:
                    search_blocks.append(node.get_index_block_pointer())
                    results.append(node)
                elif key == node.get_key():
                    continue

            #for == operator
            if operator == Operator.equal_to:
                if node.get_key() > key:
                    search_blocks.append(node.get_index_block_pointer())
                    break
                elif node.get_key() < key:
                    continue
                elif key == node.get_key():
                    search_blocks.append(node.get_index_block_pointer())
                    results.append(node)

            #for <= operator
            if operator == Operator.lesser_than_or_equal_to:
                if node.get_key() > key:
                    search_blocks.append(node.get_index_block_pointer())
                    break
                elif node.get_key() <= key:
                    search_blocks.append(node.get_index_block_pointer())
                    results.append(node)

            #for >= operator
            if operator == Operator.greater_than_or_equal_to:
                if node.get_key() < key:
                    continue
                elif node.get_key() >= key:
                    search_blocks.append(node.get_index_block_pointer())
                    results.append(node)

        dummy_pointer = block.get_dummy_node_pointer()
        node = index_nodes[-1]
        # for < operator
        if operator == Operator.lesser_than:
            if node.get_key() <= key:
                search_blocks.append(dummy_pointer)

        #for > operator
        if operator == Operator.greater_than:
            search_blocks.append(dummy_pointer)

        #for == operator
        if operator == Operator.equal_to:
            if node.get_key() <= key:
                search_blocks.append(dummy_pointer)

        #for <= operator
        if operator == Operator.lesser_than_or_equal_to:
            if node.get_key() <= key:
                search_blocks.append(dummy_pointer)

        #for >= operator
        if operator == Operator.greater_than_or_equal_to:
            search_blocks.append(dummy_pointer)


    def find_all_index_nodes(self, message):
        key = message.get_key()
        operator = message.get_operator()
        root_block_location = self.get_root_block()
        search_blocks = []
        results = []

        search_blocks.append(root_block_location)
        while len(search_blocks) > 0:
            s_block = search_blocks.pop()
            self.traverse_index_tree(s_block, key, operator, search_blocks, results)

        if message.get_return() == 'id':
            if self._index_name == 'id':
                result = [r.get_value() for r in results]
            else:
                result = [r.get_id() for r in results]
        elif message.get_return() == 'value':
            result = [r.get_value() for r in results]
        return result

    def get_all_index_nodes(self, message):
        root_block_location = self.get_root_block()
        search_blocks = []
        results = []
        search_blocks.append(root_block_location)
        while len(search_blocks) > 0:
            block_location = search_blocks.pop()
            if block_location == -1:
                continue
            block = self.read_block(block_location)
            index_nodes = block.get_index_nodes()
            for node in index_nodes:
                results.append(node)

                pointer = node.get_index_block_pointer()
                if pointer != -1:
                    search_blocks.append(pointer)

            pointer = block.get_dummy_node_pointer()
            if pointer != -1:
                search_blocks.append(pointer)
        if message.get_return() == 'id':
            result = [(r.get_key(), r.get_id()) for r in results]
        elif message.get_return() == 'value':
            result = [(r.get_key(), r.get_value()) for r in results]
        return result
