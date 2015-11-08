__author__ = 'sattvik'

import unittest
from queue import Queue
import time
import logging

from pygraphdb.services.common.communicationservice import Communication


class TestCommunicationService(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(format='%(asctime)s\t%(levelname)-10s\t%(threadName)-32s\t%(name)-32s\t%(message)s', level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')

        self.master = Communication(host='localhost', port=4545, service_name='Master', server=True)
        self.worker = Communication(host='localhost', port=4545, service_name='Worker', server=False)
        self.masterq = Queue()
        self.workerq = Queue()
        self.master.register('Q', self.masterq)
        self.worker.register('Q', self.workerq)
        self.master.startup()
        self.worker.startup()
        time.sleep(2)

    def test_send_string(self):
        self.master.send('Worker', 'Q', 'Hello World')
        message = self.workerq.get()
        self.assertEqual(message, 'Hello World')

        self.worker.send('Master', 'Q', 'Hello World')
        message = self.masterq.get()
        self.assertEqual(message, 'Hello World')

    def test_send_numbers(self):
        self.master.send('Worker', 'Q', 1234)
        message = self.workerq.get()
        self.assertEqual(message, 1234)

        self.worker.send('Master', 'Q', 12.34)
        message = self.masterq.get()
        self.assertEqual(message, 12.34)

    def tearDown(self):
        self.worker.shutdown()
        self.master.shutdown()

if __name__ == '__main__':
    unittest.main()
