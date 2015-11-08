__author__ = 'Sagar'

from pygraphdb.services.common.service import Service
from pygraphdb.services.common.socketwrapper import SocketReadWrite
from pygraphdb.services.master.querylistener import QueryListener

from queue import Queue
import socket

class ClientListener(Service):
    def construct(self, config):
        self._port = config.get('queryport', 5454)
        self._host = config.get('host', 'localhost')
        self._communication_service = config.get('communication_service', None)
        self._timeout = 1
        self._query_listeners = []

    def init(self):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.settimeout(self._timeout)
        self._socket.bind((self._host, self._port))
        self._socket.listen(5)

    def deinit(self):
        for listener in self._query_listeners:
            listener.shutdown()

    def do_work(self):
        try:
            (client_socket, address) = self._socket.accept()
        except socket.timeout:
            raise TimeoutError

        client_socket_wrapper = SocketReadWrite(client_socket)
        query_listener = QueryListener(client_socket_wrapper=client_socket_wrapper, communication_service=self._communication_service)
        query_listener.startup()
        self._query_listeners.append(query_listener)
        return True
