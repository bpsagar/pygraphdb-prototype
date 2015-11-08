__author__ = 'Sagar'
from pygraphdb.services.worker.storageservice import Node, StorageService
import logging
import time
from pygraphdb.services.common.communicationservice import CommunicationService

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

node = Node()
node._id = 1
node._type = 'Person'
node.set_property('Name', 'Sagar')
node.set_property('Age', 21)
comm_service = CommunicationService('localhost', 4545, 'Sagar')
path = "C:\\Users\\Sagar\\PycharmProjects\\pygraphdb\\pygraphdb"
storage_service = StorageService(path, comm_service)
storage_service.start()
time.sleep(3)
storage_service.add_node(node)
time.sleep(3)
storage_service.delete_node(1)
time.sleep(3)
storage_service.delete_node(1)