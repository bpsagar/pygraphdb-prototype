__author__ = 'Sagar'
from pygraphdb.services.common.service import Service
from pygraphdb.services.messages.addnode import AddNode
from pygraphdb.services.messages.deletenode import DeleteNode
from pygraphdb.services.messages.updatenode import UpdateNode
from pygraphdb.services.messages.addedge import AddEdge
from pygraphdb.services.messages.findnode import FindNode
from pygraphdb.services.messages.deleteedge import DeleteEdge
from pygraphdb.services.messages.updateedge import UpdateEdge
from pygraphdb.services.messages.addtype import AddType
from pygraphdb.services.messages.deletetype import DeleteType
from pygraphdb.services.messages.nodeindex import NodeIndex
from queue import Queue, Empty
import json
import logging
import os
from pygraphdb.services.worker.node import Node
from pygraphdb.services.worker.edge import Edge
from pygraphdb.services.worker.type import Type
from pygraphdb.services.worker.indexnode import IndexNode
from pygraphdb.services.messages.addindex import AddIndex
from pygraphdb.services.messages.deleteindex import DeleteIndex
from pygraphdb.services.messages.findindex import FindIndex

class StorageService(Service):
    def construct(self, config):
        self._directory = config.get('directory')
        self._communication_service = config.get('communication_service')
        self._node_files = {}
        self._edge_files = {}
        self._queue = Queue()
        self._type_file = 'types-list'
        self._type_fd = None

    def init(self):
        self._logger.info("Storage Service started on node [%s].", self._communication_service.get_name())

        # create a file if doesnt exist
        open((os.path.join(self._directory, self._type_file)), 'a').close()
        self._type_fd = open((os.path.join(self._directory, self._type_file)), "rb+")

    def do_work(self):
        try:
            source_name, source_service, message = self._queue.get(True, 5)
        except Empty:
            raise TimeoutError
        if isinstance(message, AddType):
            self.add_type(source_name, source_service, message.get_type())

        if isinstance(message, DeleteType):
            self.delete_type(source_name, source_service, message.get_type())

        if isinstance(message, AddNode):
            self.add_node(source_name, source_service, message.get_node(), message.get_batch())

        if isinstance(message, DeleteNode):
            self.delete_node(message.get_node().get_id(), message.get_index())

        if isinstance(message, FindNode):
            self.find_node(source_name, source_service, message)

        if isinstance(message, UpdateNode):
            self.update_node(message.get_node(), message.get_index())

        if isinstance(message, AddEdge):
            self.add_edge(source_name, source_service, message.get_edge())

        if isinstance(message, DeleteEdge):
            self.delete_edge(message.get_edge().get_id(), message.get_index())

        if isinstance(message, UpdateEdge):
            self.update_edge(message.get_edge(), message.get_index())

        return True

    def deinit(self):
        self._node_fd.close()
        self._edge_fd.close()

    def get_node_fd(self, file_name):
        if file_name in self._node_files.keys():
            return self._node_files[file_name]
        open((os.path.join(self._directory, file_name)), 'a').close()
        fd = open(os.path.join(self._directory, file_name), "rb+")
        self._node_files[file_name] = fd
        return fd

    def get_edge_fd(self, file_name):
        if file_name in self._edge_files.keys():
            return self._edge_files[file_name]
        open(os.path.join(self._directory, file_name), "a").close()
        fd = open(os.path.join(self._directory, file_name), "rb+")
        self._edge_files[file_name] = fd
        return fd

    def add_type(self, source_name, source_service, type):
        data = type.dump()
        data_size = "%08x" %len(data)
        self._type_fd.seek(0, 2)
        record = data_size + " " + data
        self._type_fd.write(bytes(record, 'UTF-8'))
        self._type_fd.flush()
        message = "Added a new type " + type.get_type() + "."
        self.send(source_name, source_service, message)
        self._logger.info(message)

    def read_type(self):
        delete_marker = '*'
        type = None
        size = 0
        while delete_marker == '*':
            size = self._type_fd.read(8).decode("UTF-8")
            if size == '':
                return (None, None)
            type_position = self._type_fd.tell()
            delete_marker = self._type_fd.read(1).decode("UTF-8")
            size = int(size, 16)
            if delete_marker == '*':
                self._type_fd.seek(size, 1)
        full_data = ''
        while size > 0:
            data = self._type_fd.read(size).decode("UTF-8")
            size -= len(data)
            full_data += data
        type = Type()
        type.load(full_data)
        return (type, type_position)

    def delete_type(self, source_name, source_service, delete_type):
        self._type_fd.seek(0, 0)
        type = None
        while True:
            type, type_position = self.read_type()
            if type is None:
                break
            if type.get_type() == delete_type.get_type():
                break

        if type is not None:
            self._type_fd.seek(type_position, 0)
            self._type_fd.write(bytes('*', 'UTF-8'))
            self._type_fd.flush()
            message = "Deleted type [" + type.get_type() + "]."
        else:
            message = "Type [" + type.get_type() + "] doesn't exist."

        self.send(source_name, source_service, message)

    def add_node(self, source_name, source_service, node, batch):
        file_name = "node-" + node.get_type().lower()
        fd = self.get_node_fd(file_name)
        data = node.dump()
        data_size = "%08x" % len(data)
        fd.seek(0, 2) # 2 - end of file reference
        location = fd.tell()
        record = data_size + " " + data
        fd.write(bytes(record, 'UTF-8'))
        fd.flush()
        properties = node.get_properties()
        index_node = IndexNode(node.get_id(), location)

        #index = AddIndex('id', index_node, node.get_type())
        #self.send(self._communication_service.get_name(), 'IndexService', index)
        #for index_name in properties.keys():
        #    index_node = IndexNode(properties[index_name], location, node.get_id())
        #    index = AddIndex(index_name, index_node, node.get_type())
        #    self.send(self._communication_service.get_name(), 'IndexService', index)
        #    name, service, msg = self._queue.get()
        index = NodeIndex(node, location)
        self.send(self._communication_service.get_name(), 'IndexService', index)
        #name, service, msg = self._queue.get()

        message = "Added a new node [id=" + str(node.get_id()) + "] to the database."
        if not batch:
            self.send(source_name, source_service, message)
        self._logger.info("Added a new node [id=%s] to the database.", node.get_id())

    def read_node(self, fd):
        delete_marker = '*'
        node = None
        size = 0
        while delete_marker == '*':
            size = fd.read(8).decode("UTF-8")
            if size == '':
                return (None, None)
            node_position = fd.tell()
            delete_marker = fd.read(1).decode("UTF-8")
            size = int(size, 16)
            if delete_marker == '*':
                fd.seek(size, 1)
        full_data = ''
        while size > 0:
            data = fd.read(size).decode("UTF-8")
            size -= len(data)
            full_data += data
        node = Node()
        node.load(full_data)
        return (node, node_position)

    def find_node(self, source_name, source_service, find_node):
        find_index = FindIndex('id', find_node.get_key(), find_node.get_type())
        self.send(self._communication_service.get_name(), 'IndexService', find_index)
        sname, sservice, location = self._queue.get()

        file_name = "node-" + find_node.get_type().lower()
        print("FILENAME", file_name)
        fd = self.get_node_fd(file_name)
        if location is None:
            self.send(source_name, source_service, None)
            return
        fd.seek(location, 0)
        node, node_position = self.read_node(fd)
        self.send(source_name, source_service, node)


    def delete_node(self, node_id, index=None):
        if index is None:
            self._node_fd.seek(0, 0) # 0 - beginning of the file reference
            node = None
            while True:
                node, node_position = self.read_node()
                if node is None:
                    break
                if node.get_id() == node_id:
                    break
        else:
            node_position = index
            self._node_fd.seek(index,0)
            node = self.read_node()

        if node:
            self._node_fd.seek(node_position, 0)
            self._node_fd.write(bytes('*', "UTF-8"))
            self._node_fd.flush()
            properties = node.get_properties()
            index_node = IndexNode(node.get_id(), node_position-8)
            index = DeleteIndex('id', index_node, node.get_type())
            self.send(self._communication_service.get_name(), 'IndexService', index)
            for index_name in properties.keys():
                index_node = IndexNode(properties[index_name], node_position-8)
                index = DeleteIndex(index_name, index_node, node.get_type())
                self.send(self._communication_service.get_name(), 'IndexService', index)
            self._logger.info("Deleted node [id=%s].", node_id)
        else:
            self._logger.warning("Could not find node [id=%s].", node_id)

    def update_node(self, node, index=None):
        self.delete_node(node.get_id(), index)
        self.add_node(node)

        self._logger.info("Updated node [id=%s].", node.get_id())

    def add_edge(self, source_name, source_service, edge):
        file_name = "edge-" + edge.get_type().lower()
        fd = self.get_edge_fd(file_name)
        data = edge.dump()
        data_size = "%08x" % len(data)
        fd.seek(0, 2) # 2 - end of file reference
        location = fd.tell()
        record = data_size + " " + data
        fd.write(bytes(record, 'UTF-8'))
        fd.flush()
        self._logger.info("Added a new edge [id=%s] to the database.", edge.get_id())
        message = "Added a new edge [id=" + str(edge.get_id()) + "] to the database."

        properties = edge.get_properties()
        index_node = IndexNode(edge.get_id(), location)
        index = AddIndex('id', index_node, edge.get_type())
        self.send(self._communication_service.get_name(), 'IndexService', index)

        index_node = IndexNode(edge.get_node1_id(), location)
        index = AddIndex('node_id1', index_node, edge.get_type())
        self.send(self._communication_service.get_name(), 'IndexService', index)

        index_node = IndexNode(edge.get_node2_id(), location)
        index = AddIndex('node_id2', index_node, edge.get_type())
        self.send(self._communication_service.get_name(), 'IndexService', index)

        index_node = IndexNode(edge.get_node1_id(), edge.get_node2_id())
        index = AddIndex('node_id1-id2', index_node, edge.get_type())
        self.send(self._communication_service.get_name(), 'IndexService', index)

        index_node = IndexNode(edge.get_node2_id(), edge.get_node1_id())
        index = AddIndex('node_id2-id1', index_node, edge.get_type())
        self.send(self._communication_service.get_name(), 'IndexService', index)

        for index_name in properties.keys():
            index_node = IndexNode(properties[index_name], location, edge.get_id())
            index = AddIndex(index_name, index_node, edge.get_type())
            self.send(self._communication_service.get_name(), 'IndexService', index)

        self.send(source_name, source_service, message)

    def read_edge(self):
        delete_marker = '*'
        edge = None
        size = 0
        while delete_marker == '*':
            size = self._edge_fd.read(8).decode("UTF-8")
            if size == '':
                return (None, None)
            edge_position = self._edge_fd.tell()
            delete_marker = self._edge_fd.read(1).decode("UTF-8")
            size = int(size, 16)
            if delete_marker == '*':
                self._edge_fd.seek(size, 1)
        full_data = ''
        while size > 0:
            data = self._edge_fd.read(size).decode("UTF-8")
            size -= len(data)
            full_data += data
        edge = Edge()
        edge.load(full_data)
        return (edge, edge_position)

    def delete_edge(self, edge_id, index=None):
        if index is None:
            self._edge_fd.seek(0, 0) # 0 - beginning of the file reference
            node = None
            while True:
                edge, edge_position = self.read_edge()
                if edge is None:
                    break
                if edge.get_id() == edge_id:
                    break
        else:
            edge_position = index
            self._edge_fd.seek(index,0)
            edge = self.read_edge()

        if edge:
            self._edge_fd.seek(edge_position, 0)
            self._edge_fd.write(bytes('*', "UTF-8"))
            self._edge_fd.flush()
            self._logger.info("Deleted edge [id=%s].", edge_id)
        else:
            self._logger.warning("Could not find edge [id=%s].", edge_id)

    def update_edge(self, edge, index=None):
        self.delete_edge(edge.get_id(), index)
        self.add_edge(edge)
        self._logger.info("Updated edge [id=%s].", edge.get_id())

    def stop(self):
        self._running = False
        self._type_fd.close()
        for fd in self._node_files.values():
            fd.close()
        for fd in self._edge_files():
            fd.close()

        self._logger.info("Storage Service stopped on node [%s].", self._communication_service.get_name())

    def get_queue(self):
        return self._queue
