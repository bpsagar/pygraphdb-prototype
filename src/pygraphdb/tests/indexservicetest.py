__author__ = 'Sagar'
from pygraphdb.services.worker.indexservice import IndexService
from pygraphdb.services.messages.addindex import AddIndex
from pygraphdb.services.messages.deleteindex import DeleteIndex
from pygraphdb.services.messages.findindex import FindIndex
from pygraphdb.services.messages.findallindex import Operator, FindAllIndex
from pygraphdb.services.worker.indexnode import IndexNode
import unittest
import time
import random
import logging

class IndexServiceTest(unittest.TestCase):
    def setUp(self):
        #logging.basicConfig(format='%(asctime)s\t%(levelname)-10s\t%(threadName)-32s\t%(name)-32s\t%(message)s', level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')
        self.index_service = IndexService(block_size=2048, index_directory="C:\\Users\\Sagar\\PycharmProjects\\pygraphdb\\pygraphdb\\index")
        self.index_service.startup()
        self._keys = []

    def large_index(self):
        file = "C:\\Users\\Sagar\\PycharmProjects\\pygraphdb\\pygraphdb\\index\\keys"
        fd = open(file, 'wb+')
        for i in range(10):
            key = self.random_key_generator()
            value = self.get_value_from_key(key)
            #fd.write(bytes(key, 'UTF-8'))
            #fd.write(bytes("\n", 'UTF-8'))
            self._keys.append(key)
            index_node = IndexNode(key, value)
            index = AddIndex('name', index_node, 'Movie')
            self.index_service._queue.put(('Master', 'IndexService', index))
        while self.index_service._queue.qsize() > 0:
            time.sleep(1)
            print(self.index_service._queue.qsize())

    def read_from_file(self):
        file = "C:\\Users\\Sagar\\PycharmProjects\\pygraphdb\\pygraphdb\\index\\keys"
        fd = open(file, 'rb+')
        for i in range(10000):
            c = fd.read(1).decode('utf-8')
            key = ''
            while c != '\n':
                key += c
                c = fd.read(1).decode('utf-8')
            self._keys.append(key)

        for i in range(10000):
            key = self._keys[i]
            value = self.get_value_from_key(key)
            index_node = IndexNode(key, value)
            index = AddIndex('name', index_node)
            self.index_service._queue.put(('Master', 'IndexService', index))

        while self.index_service._queue.qsize() > 0:
            time.sleep(1)
            print(self.index_service._queue.qsize())

    def read(self):
        for i in range(10000):
            key = 'ivrnfckfuzsjfknwpsg'
            find = FindIndex('name', key)
            self.index_service._queue.put(('Master', 'IndexService', find))
        while self.index_service._queue.qsize() > 0:
            time.sleep(1)
            print(self.index_service._queue.qsize())

    def find_index(self):
        self.large_index()
        count = 0
        for key in self._keys:
            find = FindIndex('name', key)
            self.index_service._queue.put(('Master', 'IndexService', find))
            index_node = IndexNode(key, None, None)
            delete = DeleteIndex('name', index_node)
            self.index_service._queue.put(('Master', 'IndexService', delete))
            self.index_service._queue.put(('Master', 'IndexService', find))
            count += 1
        while self.index_service._queue.qsize() > 0:
            time.sleep(1)
            print(self.index_service._queue.qsize())

    def test_find_all_index(self):
        #self.large_index()
        find = FindAllIndex('name', None, 'Movie', None)
        self.index_service._queue.put(('Master', 'IndexService', find))
        while self.index_service._queue.qsize() > 0:
            time.sleep(1)
            print("Finding...")

    def index(self):
        key = 'adbquwjzsvlvooy'
        find = FindIndex('name', key)
        self.index_service._queue.put(find)
        index_node = IndexNode(key, None, None)
        delete = DeleteIndex('name', index_node)
        self.index_service._queue.put(delete)
        self.index_service._queue.put(find)
        while self.index_service._queue.qsize() > 0:
            time.sleep(1)
        #print(self.index_service._queue.qsize())
        #print(self.get_value_from_key(key))
        #self.index_service.join()


    def random_key_generator(self):
        no_of_chars = random.randint(10,20)
        s = ''
        chars = 'abcdefghijklmnopqrstuvwxyz'
        for i in range(no_of_chars):
            s += random.choice(chars)
        return s

    def get_value_from_key(self, s):
        key = 0
        for i in s :
            key += ord(i)
        return key

    def tearDown(self):
        self.index_service.shutdown()


if __name__ == '__main__':
    unittest.main()