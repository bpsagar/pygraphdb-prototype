__author__ = 'Sagar'
from pygraphdb.services.common.communicationservice import Communication
from pygraphdb.services.common.heartbeatservice import HeartBeatService
from pygraphdb.services.worker.node import Node
from pygraphdb.services.messages.addnode import AddNode
from pygraphdb.services.messages.updatenode import UpdateNode
from pygraphdb.services.messages.deletenode import DeleteNode
from pygraphdb.services.worker.edge import Edge
from pygraphdb.services.messages.addedge import AddEdge
from pygraphdb.services.messages.deleteedge import DeleteEdge
from pygraphdb.services.messages.updateedge import UpdateEdge
from pygraphdb.services.master.clientlistener import ClientListener
from pygraphdb.services.master.queryprocessor import QueryProcessor
from pygraphdb.services.master.queryplanner import QueryPlanner
import time
import configparser

class MasterProcess(object):
    def __init__(self,):
        super(MasterProcess, self).__init__()
        self._name = None
        self._port = None
        self._hostname = None
        self._services = {}

    def init(self, config_path):
        config = configparser.ConfigParser(allow_no_value=True)
        config.read(config_path)
        self._name = config['Node']['Name']
        self._port = int(config['Node']['Port'])
        self._hostname = config['Node']['Hostname']

    def run(self):
        communication_service = Communication(host=self._hostname, port=self._port, service_name=self._name, server=True)
        communication_service.startup()
        self._services['CommunicationService'] = communication_service
        communication_service.register('CommunicationService', communication_service.get_queue())

        #heartbeat_service = HeartBeatService(communication_service=communication_service, interval=10)
        #communication_service.register('HeartBeatService', heartbeat_service.get_queue())
        #heartbeat_service.startup()
        #self._services['HeartBeatService'] = heartbeat_service

        client_listener = ClientListener(communication_service=communication_service, queryport=5454)
        client_listener.startup()
        self._services['ClientListener'] = client_listener

        query_processor = QueryProcessor(communication_service=communication_service)
        query_processor.startup()
        communication_service.register('QueryProcessor', query_processor.get_queue())
        self._services['QueryProcessor'] = query_processor
        
        query_planner = QueryPlanner(query_processor=query_processor, communication_service=communication_service)
        query_planner.startup()
        communication_service.register('QueryPlanner', query_planner.get_queue())
        self._services['QueryPlanner'] = query_planner
        #query_planner = QueryPlanner(communication_service=communication_service)
        #query_planner.startup()
        #communication_service.register('QueryPlanner', query_planner.get_queue())
        #self._services['QueryPlanner'] = query_planner

    def stop(self):
        for service in self._services.values():
            service.shutdown()

    def test_node(self):
        node = Node()
        node._id = 1
        node._type = 'Person'
        node.set_property('Name', 'Sagar')
        node.set_property('Age', '21')
        time.sleep(3)
        add_node = DeleteNode(node)
        self._services['CommunicationService'].send('Worker', 'StorageService', add_node)

    def test_edge(self):
        edge = Edge()
        edge._id = 10
        edge._type = 'Relation'
        edge._node1_id = 1
        edge._node2_id = 2
        edge.set_property('Name', 'Sagar')
        edge.set_property('Age', 21)
        time.sleep(3)
        update_edge = UpdateEdge(edge)
        self._services['CommunicationService'].send('WORKER1', 'StorageService', update_edge)