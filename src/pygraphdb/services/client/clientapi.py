__author__ = 'Sagar'
from pygraphdb.services.common.socketwrapper import SocketReadWrite
import socket
class ClientAPI(object):
    def __init__(self):
        super(ClientAPI, self).__init__()
        self._host = 'localhost'
        self._port = 5454
        self._name = 'Client'
        self._socket_wrapper = None
        self._cursor_objects = []
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket_wrapper = SocketReadWrite(s)
        s.connect((self._host, self._port))

    def connect(self):
        self._socket_wrapper.write('CONNECT')
        message = self._socket_wrapper.read()
        if message != 'CLIENTNAME?':
            return
        self._socket_wrapper.write(self._name)
        message = self._socket_wrapper.read()

    def execute(self, query):
        self._socket_wrapper.write('EXECUTE')
        message = self._socket_wrapper.read()
        if message != 'QUERY?':
            return
        self._socket_wrapper.write(query)
        query_id = self._socket_wrapper.read()
        self._socket_wrapper.write('OK')
        message = self._socket_wrapper.read()
        #print(message)
        cursor_object = Cursor(query_id, self._socket_wrapper)
        self._cursor_objects.append(cursor_object)
        return message
        #return cursor_object

    def disconnect(self):
        self._socket_wrapper.close()

class Cursor(object):
    def __init__(self, query_id, socket_wrapper):
        super(Cursor, self).__init__()
        self._query_id = query_id
        self._socket_wrapper = socket_wrapper

    def has_next(self):
        self._socket_wrapper.write('HASNEXT?' + self._query_id)
        message = self._socket_wrapper.read()
        if message == 'TRUE':
            return True
        else:
            return False

    def fetch_next_row(self):
        self._socket_wrapper.write('FETCHNEXTROW' + self._query_id)
        message = self._socket_wrapper.read()
        return message

