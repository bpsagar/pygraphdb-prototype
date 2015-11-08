__author__ = 'Sagar'
import logging
from queue import Queue, Empty

from pygraphdb.services.common.service import Service
from pygraphdb.services.common.handlers.connectionhandler import ConnectionHandler
from pygraphdb.services.common.handlers.clientconnectionhandler import ClientConnectionHandler
from pygraphdb.services.common.heartbeatservice import DeadNode


class Communication(Service):
    # Service functions
    def construct(self, config):
        self._service_queues = {}
        self._clients = {}
        self._queue = Queue()
        self._server = config.get('server', False)
        self._host = config.get('host', 'localhost')
        self._port = config.get('port', 4545)
        self._timeout = 1
        self._last_added_client_index = -1

        if Service.COMMUNICATION_INSTANCE is None:
            Service.COMMUNICATION_INSTANCE = self

    def init(self):
        self._handler = None
        if self._server:
            connection_handler = ConnectionHandler(
                host=self._host, port=self._port,
                service_name='%s[ConnectionHandler]' % self._service_name,
                node_name = Service.COMMUNICATION_INSTANCE.get_name(),
                parent=self)
            self._handler = connection_handler
            connection_handler.startup()
        else:
            client_connection_handler = ClientConnectionHandler(
                host=self._host, port=self._port, node_name=Service.COMMUNICATION_INSTANCE.get_name(),
                service_name='%s[ConnectionHandler]' % self._service_name,
                parent=self)
            self._handler = client_connection_handler
            client_connection_handler.startup()

    def deinit(self):
        self._handler.shutdown()

    def do_work(self):
        try:
            source_name, source_service, message = self._queue.get(True, self._timeout)
        except Empty:
            raise TimeoutError

        if isinstance(message, DeadNode):
            if self._server:
                self.remove_client(message.get_client())
                self._logger.info("Removed client [%s] from client list.", message.get_client().get_name())
            else:
                self.remove_client(message.get_client())
                self._logger.info("Removed client [%s] from client list.", message.get_client().get_name())
        return True

    # Helper functions
    def add_client(self, client):
        self._clients[client.get_name()] = client
        self._logger.info('New client [%s] connected.', client.get_name())

    def send(self, source_service, target_name, target_service, message):
        self._handler.send(self._service_name, source_service, target_name, target_service, message)

    def get_name(self):
        return self._service_name

    def get_client(self, client_name):
        return self._clients[client_name]

    def register(self, service_name, q):
        self._service_queues[service_name] = q

    def get_service_queue(self, service_name):
        return self._service_queues.get(service_name, None)

    def get_client_list(self):
        return self._clients.values()

    def remove_client(self, client):
        if client.get_name() in self._clients.keys():
            self._clients.pop(client.get_name())

    def get_queue(self):
        return self._queue

    def get_next_client(self):
        client_list = []
        for client in self.get_client_list():
            client_list.append(client)
        self._last_added_client_index = (self._last_added_client_index + 1) % len(client_list)
        return client_list[self._last_added_client_index]
