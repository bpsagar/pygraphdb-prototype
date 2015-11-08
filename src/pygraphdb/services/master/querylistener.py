__author__ = 'Sagar'
from pygraphdb.services.common.service import Service
from pygraphdb.services.master.queryparser import QueryParser
from pygraphdb.services.master.queryprocessor import QueryProcessor
from pygraphdb.services.master.queryparser import InsertNodeQuery, CreateTypeQuery
import socket
from queue import Queue
import json

class QueryListener(Service):
    def construct(self, config):
        self._client_socket_wrapper = config.get('client_socket_wrapper', None)
        self._communication_service = config.get('communication_service', None)
        self._client_name = None
        self._queue = Queue()
        self._query_id = None

    def init(self):
        self._communication_service.register(self._service_name, self._queue)

    def do_work(self):
        try:
            message = self._client_socket_wrapper.read()
        except socket.timeout:
            raise TimeoutError
        except ValueError:
            raise TimeoutError

        self._logger.info("Got message: %s.", message)
        if message == 'EXECUTE':
            self.execute()
        elif message == 'CONNECT':
            self.handshake()
        elif message == 'HASNEXT?':
            self.has_next()
        elif message == 'FETCHNEXTROW':
            self.fetch_next_row()
        elif message == 'DISCONNECT':
            self.disconnect()
        return True

    def execute(self):
        self._client_socket_wrapper.write('QUERY?')
        query = self._client_socket_wrapper.read()
        query_parser = QueryParser()
        try:
            query_obj = query_parser.parse(query)
        except:
            self._client_socket_wrapper.write(str(-1))
            message = self._client_socket_wrapper.read()
            message = 'Error occurred while parsing the query.'
            data = json.dumps(message)
            self._client_socket_wrapper.write(data)
            return


        self._query_id = query_obj.get_query_id()
        self._client_socket_wrapper.write(self._query_id)
        self.send(self._communication_service.get_name(), 'QueryProcessor', query_obj)
        #query_processor = QueryProcessor(query_obj, self._communication_service, self)
        #query_processor.process()
        message = self._client_socket_wrapper.read()
        if message != 'OK':
            return

        source_name, source_service, message = self._queue.get()
        data = json.dumps(message)
        self._client_socket_wrapper.write(data)

    def handshake(self):
        self._client_socket_wrapper.write('CLIENTNAME?')
        self._client_name = self._client_socket_wrapper.read()
        self._client_socket_wrapper.write('OK')

    def has_next(self):
        pass

    def fetch_next_row(self):
        pass

    def disconnect(self):
        pass
