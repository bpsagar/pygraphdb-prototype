__author__ = 'Sagar'
import logging
import socket

from pygraphdb.services.common.socketwrapper import SocketReadWrite
from pygraphdb.services.common.handlers.receivehandler import ReceiveHandler
from pygraphdb.services.common.handlers.sendhandler import SendHandler
from pygraphdb.services.common.client import Client
from pygraphdb.services.common.service import Service

class ConnectionHandler(Service):

    def construct(self, config):
        self._host = config.get('host', 'localhost')
        self._port = config.get('port', 4545)
        self._node_name = config.get('node_name')
        self.socket = None
        self._handlers = []
        self._clients = []
        self._timeout = 1

    def init(self):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.settimeout(self._timeout)
        self._socket.bind((self._host, self._port))
        self._socket.listen(5)

    def deinit(self):
        for client in self._clients:
            client.close()
        self._socket.close()

    def do_work(self):
        try:
            (client_socket, address) = self._socket.accept()
        except socket.timeout:
            raise TimeoutError

        self._logger.info('New client request received from [%s:%d]', address[0], address[1])
        client_socket_wrapper = SocketReadWrite(client_socket)
        client = self.handshake(client_socket_wrapper)
        self._clients.append(client)
        self.get_parent().add_client(client)

        receive_handler = ReceiveHandler(parent=self, socket_wrapper=client_socket_wrapper, client=client)
        receive_handler.start()
        self._handlers.append(receive_handler)

        send_handler = SendHandler(parent=self, client=client)
        send_handler.startup()
        self._handlers.append(send_handler)

        client.set_handlers(receive_handler, send_handler)
        return True

    def send(self, source_name, source_service, target_name, target_service, message):
        if target_name == self.get_parent().get_name():
            self.get_parent().get_service_queue(target_service).put((source_name, source_service, message))
        else:
            self.get_parent().get_client(target_name).get_send_handler().send(source_name, source_service, target_name, target_service, message)

    def handshake(self, client_socket_wrapper):
        client_info = client_socket_wrapper.read()
        client_socket_wrapper.write(self._node_name)
        client = Client(client_info, client_socket_wrapper)
        self._logger.info('Handshake complete with client [%s].', client.get_name())
        return client
