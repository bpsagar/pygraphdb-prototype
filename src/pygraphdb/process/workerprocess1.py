__author__ = 'Sagar'
from pygraphdb.services.common.communicationservice import Communication
from pygraphdb.services.common.heartbeatservice import HeartBeatService
from pygraphdb.services.worker.storageservice import StorageService
from pygraphdb.services.worker.indexservice import IndexService
from pygraphdb.services.messages.addnode import AddNode
from pygraphdb.services.worker.node import Node
import configparser

class WorkerProcess(object):
    def __init__(self):
        super(WorkerProcess, self).__init__()
        self._name = None
        self._master_port = None
        self._master_hostname = None
        self._services = {}

    def init(self, config_path):
        config = configparser.ConfigParser(allow_no_value=True)
        config.read(config_path)
        self._name = config['Node']['Name']
        self._master_port = int(config['Node']['MasterNodePort'])
        self._master_hostname = config['Node']['MasterNodeHostname']
        self._data_directory = config['Node']['DataDirectory']
        self._index_directory = config['Node']['IndexDirectory']

    def run(self):
        communication_service = Communication(host=self._master_hostname, port=self._master_port, service_name=self._name, server=False)
        communication_service.startup()
        self._services['CommunicationService'] = communication_service
        communication_service.register('CommunicationService', communication_service.get_queue())

        #heartbeat_service = HeartBeatService(communication_service=communication_service, interval=5)
        #communication_service.register('HeartBeatService', heartbeat_service.get_queue())
        #heartbeat_service.startup()
        #self._services['HeartBeatService'] = heartbeat_service

        storage_service = StorageService(directory=self._data_directory, communication_service=communication_service)
        communication_service.register('StorageService', storage_service.get_queue())
        storage_service.startup()
        self._services['StorageService'] = storage_service

        index_service = IndexService(block_size=1024, index_directory=self._index_directory, communication_service=communication_service)
        communication_service.register('IndexService', index_service.get_queue())
        index_service.startup()
        self._services['IndexService'] = index_service


        node = Node()
        node._id = 1
        node._type = 'Person'
        node._properties['Name'] = 'Sagar'
        node._properties['Age'] = 21
        add_node = AddNode(node)
        #communication_service.send(communication_service.get_name(), 'StorageService', add_node)

    def stop(self):
        for service in self._services.values():
            service.shutdown()