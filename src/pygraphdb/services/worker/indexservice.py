__author__ = 'Sagar'
from pygraphdb.services.common.service import Service
from pygraphdb.services.messages.addindex import AddIndex
from pygraphdb.services.messages.deleteindex import DeleteIndex
from pygraphdb.services.messages.findindex import FindIndex
from pygraphdb.services.messages.findallindex import FindAllIndex
from pygraphdb.services.worker.indexer import Indexer
from pygraphdb.services.messages.nodeindex import NodeIndex
from pygraphdb.services.worker.indexnode import IndexNode
from queue import Queue, Empty
from struct import pack, unpack
from threading import Thread

import os

class IndexService(Service):
    def init(self):
        #self._logger.info("Index Service started on node [%s].", self._communication_service.get_name())
        pass

    def construct(self, config):
        self._block_size = config.get('block_size', 1024)
        self._communication_service = config.get('communication_service', None)
        self._queue = Queue()
        self._timeout = 1
        self._index_directory = config.get('index_directory')
        self._indexing_objects = {}

    def deinit(self):
        for obj in self._indexing_objects.values():
            obj.stop()

    def do_work(self):
        try:
            source_name, source_service, message = self._queue.get(True, self._timeout)
        except Empty:
            raise TimeoutError

        if isinstance(message, AddIndex):
            index_name = message.get_index_name()
            type = message.get_type()
            file_name = type + "-" + index_name
            if file_name not in self._indexing_objects.keys():
                property_object = Indexer(index_name, self._index_directory, type, self._block_size)
                self._indexing_objects[index_name] = property_object
            self._indexing_objects[index_name].add_index(message.get_index_node(), None)
            message = "Added index type = [%s] index_name = [%s]" %(type, index_name)
            self._logger.info(message)
            self.send(source_name, source_service, message)

        if isinstance(message, NodeIndex):
            node = message.get_node()
            location = message.get_location()
            properties = node.get_properties()
            indexes = []
            index_node = IndexNode(node.get_id(), location)
            index = AddIndex('id', index_node, node.get_type())
            indexes.append(index)
            for index_name in properties.keys():
                index_node = IndexNode(properties[index_name], location, node.get_id())
                index = AddIndex(index_name, index_node, node.get_type())
                indexes.append(index)

            threads = []

            for index in indexes:
                index_name = index.get_index_name()
                type = index.get_type()
                file_name = type + "-" + index_name
                if file_name not in self._indexing_objects.keys():
                    property_object = Indexer(index_name, self._index_directory, type, self._block_size)
                    self._indexing_objects[index_name] = property_object
                #self._indexing_objects[index_name].add_index(index.get_index_node(), None)
                thread = Thread(target=self._indexing_objects[index_name].add_index, args=(index.get_index_node(), None))
                thread.start()
                threads.append(thread)

            for thread in threads:
                thread.join()

            message = "Added indexes to node [id=" + str(node.get_id()) + "] to the database."
            self._logger.info(message)
            #self.send(source_name, source_service, message)


        if isinstance(message, FindIndex):
            index_name = message.get_index_name()
            type = message.get_type()
            file_name = type + "-" + index_name
            if file_name not in self._indexing_objects.keys():
                property_object = Indexer(index_name, self._index_directory, type, self._block_size)
                self._indexing_objects[index_name] = property_object
            location = self._indexing_objects[index_name].find_index_node(message.get_key())
            self.send(source_name, source_service, location)

        if isinstance(message, DeleteIndex):
            index_name = message.get_index_name()
            type = message.get_type()
            file_name = type + "-" + index_name
            if file_name not in self._indexing_objects.keys():
                property_object = Indexer(index_name, self._index_directory, type, self._block_size)
                self._indexing_objects[index_name] = property_object
            self._indexing_objects[index_name].delete_index_node(message.get_index_node())

        if isinstance(message, FindAllIndex):
            index_name = message.get_index_name()
            type = message.get_type()
            file_name = type + "-" + index_name
            if file_name not in self._indexing_objects.keys():
                property_object = Indexer(index_name, self._index_directory, type, self._block_size)
                self._indexing_objects[index_name] = property_object

            if message.get_operator() is None:
                results = self._indexing_objects[index_name].get_all_index_nodes(message)
            else:
                results = self._indexing_objects[index_name].find_all_index_nodes(message)
            self.send(source_name, source_service, results)
        return True

    def get_queue(self):
        return self._queue