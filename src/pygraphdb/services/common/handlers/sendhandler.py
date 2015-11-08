__author__ = 'Sagar'
import logging
from queue import Queue, Empty

from pygraphdb.services.common.service import Service

class SendHandler(Service):

    def construct(self, config={}):
        self._communication_service = self.get_parent().get_parent()
        self._queue = Queue()
        self._client = config.get('client', None)

    def init(self):
        self._logger.info('Starting Send Handler for node [%s].', self._client.get_name())

    def deinit(self):
        pass

    def do_work(self):
        try:
            (source_name, source_service, target_name, target_service, message) = self._queue.get(True, 5)
        except Empty:
            raise TimeoutError
        if target_name == self._communication_service.get_name() or target_name is None:
            self._logger.debug('Sending message [%s] to target service [%s].', str(message), target_service)
            self._communication_service.get_service_queue(target_service).put((source_name, source_service, message))
        else:
            self._logger.debug('Sending message [%s] to target service [%s] on node [%s].', str(message), target_service, target_name)
            self._communication_service.get_client(target_name).send_message(source_name, source_service, target_service, message)
        return True

    def send(self, source_name, source_service, target_name, target_service, message):
        self._queue.put((source_name, source_service, target_name, target_service, message))

