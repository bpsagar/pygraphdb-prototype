__author__ = 'Sagar'
from pygraphdb.services.master.queryparser import InsertNodeQuery, CreateTypeQuery, DeleteTypeQuery, FindNodeQuery,InsertEdgeQuery
from pygraphdb.services.worker.node import Node
from pygraphdb.services.worker.type import Type
from pygraphdb.services.messages.addnode import AddNode
from pygraphdb.services.messages.addtype import AddType
from pygraphdb.services.messages.deletetype import DeleteType
from pygraphdb.services.common.service import Service
from pygraphdb.services.master.queryplanner import QueryPlanner
from queue import Queue, Empty
import time

class QueryProcessor(Service):
    def construct(self, config):
        self._communication_service = config.get('communication_service', None)
        self._queue = Queue()

    def init(self):
        pass

    def deinit(self):
        pass

    def do_work(self):
        try:
            source_name, source_service, message = self._queue.get(True, 5)
        except Empty:
            raise TimeoutError
        self.process(source_name, source_service, message)
        return True

    def process(self, source_name, source_service, message):
        if isinstance(message, InsertNodeQuery):
            node = Node(time.time(), message.get_type(), message.get_properties())
            add_node = AddNode(node, message.get_batch())
            client = self._communication_service.get_next_client()
            self.send(client.get_name(), 'StorageService', add_node)
            if message.get_batch() == False:
                s_name, s_service, message = self._queue.get()
            else:
                message = "Node added."


        if isinstance(message, CreateTypeQuery):
            type = Type(message.get_type(), message.get_parent_type(), message.get_description())
            add_type = AddType(type)
            message = ''
            for client in self._communication_service.get_client_list():
                self.send(client.get_name(), 'StorageService', add_type)
                s_name, s_service, msg = self._queue.get()
                message += msg + '\n'

        if isinstance(message, DeleteTypeQuery):
            type = Type(message.get_type())
            delete_type = DeleteType(type)
            message = ''
            for client in self._communication_service.get_client_list():
                self.send(client.get_name(), 'StorageService', delete_type)
                s_name, s_service, msg = self._queue.get()
                message += msg + '\n'

        if isinstance(message, FindNodeQuery):
            #query_planner = QueryPlanner(query=message, query_processor=self, communication_service=self._communication_service)
            #query_planner.startup()
            #self._communication_service.register('QueryPlanner', query_planner.get_queue())
            self.send(self._communication_service.get_name(), 'QueryPlanner', message)
            s_name, s_service, message = self._queue.get()

        if isinstance(message, InsertEdgeQuery):
            #query_planner = QueryPlanner(query=message, query_processor=self, communication_service=self._communication_service)
            #query_planner.startup()
            #self._communication_service.register('QueryPlanner', query_planner.get_queue())
            self.send(self._communication_service.get_name(), 'QueryPlanner', message)
            s_name, s_service, message = self._queue.get()

        self.send(source_name, source_service, message)

    def get_queue(self):
        return self._queue
