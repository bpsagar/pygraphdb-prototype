__author__ = 'Sagar'
import logging
from pickle import dumps

class Client(object):
    def __init__(self, name, socket_wrapper):
        super(Client, self).__init__()
        self._name = name
        self._socket_wrapper = socket_wrapper
        self._logger = logging.getLogger(self.__class__.__name__)
        self._receive_handler = None
        self._send_handler = None

    def get_name(self):
        return self._name

    def send_message(self,source_name, source_service, target_service, message):
        data = { 'target_service': target_service, 'message': message, 'source_service':source_service, 'source_name':source_name }
        self._socket_wrapper.write_bytes(dumps(data))

    def set_handlers(self, receive_handler, send_handler):
        self._receive_handler = receive_handler
        self._send_handler = send_handler

    def get_send_handler(self):
        return self._send_handler

    def close(self):
        self._socket_wrapper.close()
        self._logger.info("Client [%s] connection closed.", self._name)
