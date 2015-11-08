__author__ = 'Sagar'
from threading import Thread
import socket
import logging
import time

from pygraphdb.services.common.socketwrapper import SocketReadWrite
from pygraphdb.services.common.handlers.receivehandler import ReceiveHandler
from pygraphdb.services.common.handlers.sendhandler import SendHandler
from pygraphdb.services.common.client import Client
from pygraphdb.services.common.service import Service


class ClientConnectionHandler(Service):

    def construct(self, config={}):
        self._host = config.get('host', 'localhost')
        self._port = config.get('port', 4545)
        self._socket_wrapper = None
        self._send_handler = None
        self._receive_handler = None
        self._node_name = config.get('node_name', 'Worker')
        self._master_node_name = None
        self._timeout = 1

    def init(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket_wrapper = SocketReadWrite(s)
        self._logger.info('Connecting to master node.')
        s.connect((self._host, self._port))
        client = self.handshake(self._socket_wrapper)
        self.get_parent().add_client(client)
        self._logger.info('Connection to master node successful.')
        self._receive_handler = ReceiveHandler(parent=self, socket_wrapper=self._socket_wrapper, client=client)
        self._receive_handler.start()
        self._send_handler = SendHandler(parent=self, client=client)
        self._send_handler.start()

    def deinit(self):
        self._socket_wrapper.close()

    def do_work(self):
        time.sleep(self._timeout)
        raise TimeoutError

    def send(self, source_name, source_service, target_name, target_service, message):
        self._send_handler.send(source_name, source_service, target_name, target_service, message)

    def handshake(self, socket_wrapper):
        socket_wrapper.write(self._node_name)
        self._master_node_name = socket_wrapper.read()
        client = Client(self._master_node_name, socket_wrapper)
        self._logger.info('Handshake complete with the master node [%s]', self._master_node_name)
        return client
