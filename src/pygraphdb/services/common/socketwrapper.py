__author__ = 'Sagar'

class SocketReadWrite:
    """Class that converts a socket into reliable read/write handler"""
    def __init__(self, sock):
        self._s = sock

    def set_timeout(self, timeout):
        self._s.settimeout(timeout)

    def read(self):
        size = self._s.recv(8).decode("UTF-8")
        size = int(size, 16)
        fulldata = ''
        while size > 0:
            data = self._s.recv(size).decode("UTF-8")
            size -= len(data)
            fulldata += data
        return fulldata

    def write(self, data):
        size = "%08x" % len(data)
        self._s.send(bytes(size, 'UTF-8'))
        self._s.send(bytes(data, 'UTF-8'))

    def read_bytes(self):
        size = self._s.recv(8).decode("UTF-8")
        if size == '':
            return b''
        size = int(size, 16)
        fulldata = b''
        while size > 0:
            data = self._s.recv(size)
            size -= len(data)
            fulldata += data
        return fulldata

    def write_bytes(self, data):
        size = "%08x" % len(data)
        self._s.send(bytes(size, 'UTF-8'))
        self._s.send(data)

    def close(self):
        self._s.close()