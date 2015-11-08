__author__ = 'Sagar'
import logging
from pickle import loads
import socket
import time

from pygraphdb.services.common.service import Service

class ReceiveHandler(Service):
    def construct(self, config={}):
        self._communication_service = self.get_parent().get_parent()
        self._socket_wrapper = config.get('socket_wrapper', None)
        self._client = config.get('client', None)

    def init(self):
        self._logger.info('Starting Receive Handler for node [%s].', self._client.get_name())
        self._socket_wrapper.set_timeout(5)

    def deinit(self):
        pass

    def do_work(self):
        try:
            data = loads(self._socket_wrapper.read_bytes())
        except socket.timeout:
            raise TimeoutError
        except EOFError:
            self._logger.error("EOF occurred while reading incoming message. Handler will go down!")
            return False
        self._logger.debug('Received message [%s] for the service [%s].', str(data['message']), data['target_service'])
        self._communication_service.get_service_queue(data['target_service']).put((data['source_name'], data['source_service'], data['message']))
        return True