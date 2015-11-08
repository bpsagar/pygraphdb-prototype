__author__ = 'Sagar'
from pygraphdb.services.common.service import Service
from pygraphdb.services.master.queryparser import InsertEdgeQuery, FindNodeQuery
from pygraphdb.services.messages.findallindex import FindAllIndex, Operator
from pygraphdb.services.messages.findnode import FindNode
from pygraphdb.services.master.expressiontreenode import ExpressionTreeNode
from pygraphdb.services.messages.addedge import AddEdge
from pygraphdb.services.worker.edge import Edge
import time

from queue import Queue, Empty

class ResultSet(object):
    def __init__(self, type, results):
        self._type = type
        self._results = results

    def get_type(self):
        return self._type

    def get_results(self):
        return self._results

class QueryPlanner(Service):
    def construct(self, config={}):
        self._query = config.get('query')
        self._queue = Queue()
        self._communication_service = config.get('communication_service')
        self._query_processor = config.get('query_processor')
        self._node_name = {}
        self._name_type = {}
        self._ids_list = {}
        self._cache_nodes = {}
        self._cache_edge_list = {}

    def init(self):
        pass

    def deinit(self):
        pass

    def do_work(self):
        try:
            source_name, source_service, query = self._queue.get(True, 5)
        except Empty:
            raise TimeoutError

        self._query = query

        if isinstance(self._query, InsertEdgeQuery):
            results = self.insert_edge_planner()

            msg = ''
            if len(results) == 0:
                msg = 'No edges added.'
            for id1, id2 in results:
                edge = Edge(time.time(), id1, id2, self._query.get_type())
                new_edge = AddEdge(edge)
                client = self._communication_service.get_next_client()
                self.send(client.get_name(), 'StorageService', new_edge)
                source_name, source_service, message = self._queue.get()
                #for client in self._communication_service.get_client_list():
                #    self.send(client.get_name(), 'StorageService', new_edge)
                #    source_name, source_service, message = self._queue.get()
                msg += client.get_name() + ":" + str(id1) + "->" + str(id2) + "   " + message + "\n"

            self.send(self._communication_service.get_name(), 'QueryProcessor', msg)
        if isinstance(self._query, FindNodeQuery):
            result = self.execution_planner()
            self.send(self._communication_service.get_name(), 'QueryProcessor', result)
        return True

    def get_queue(self):
        return self._queue

    def set_basic_info_from_graph(self):
        query_graph = self._query.get_query_graph().get_node_edge_list()
        self._name_type = {}
        self._node_name = {}
        self._node_name['node1'] = query_graph[0]
        self._name_type[query_graph[0]] = query_graph[1]
        if len(query_graph) == 2:
            return
        self._node_name['edge'] = query_graph[2]
        self._node_name['node2'] = query_graph[4]
        self._name_type[query_graph[2]] = query_graph[3]
        self._name_type[query_graph[4]] = query_graph[5]

    def common_results(self, result1, result2):
        r1 = set(result1)
        r2 = set(result2)
        r = set.intersection(r1,r2)
        return list(r)

    def combine_results(self, result1, result2):
        r1 = set(result1)
        r2 = set(result2)
        r = set.union(r1,r2)
        return list(r)

    def is_variable(self, value):
        l = str(value).split(".")
        if len(l) != 2:
            return False
        if l[0] in self._name_type.keys():
            return True
        return False

    def fetch_ids(self, tree_node):
        key = tree_node.get_right_node().get_value()
        try:
            key = int(key)
        except:
            pass
        var, name = tree_node.get_left_node().get_value().split(".")
        operator = Operator.get_operator(tree_node.get_value())
        find = FindAllIndex(name, key, self._name_type[var], operator, "id")
        res = []
        for client in self._communication_service.get_client_list():
            self.send(client.get_name(), 'IndexService', find)

        for client in self._communication_service.get_client_list():
            source_name, source_service, results = self._queue.get()
            res += results
        return res

    def fetch_results(self, tree_node):
        results_ids = []

        if self.is_variable(tree_node.get_left_node().get_value()) and self.is_variable(tree_node.get_right_node().get_value()):
            name, var = tree_node.get_right_node().get_value().split(".")
            find = FindAllIndex(var, None, self._name_type[name], None, "id")
            results = []
            if self._name_type[name] not in self._ids_list.keys():
                for client in self._communication_service.get_client_list():
                    self.send(client.get_name(), 'IndexService', find)

                for client in self._communication_service.get_client_list():
                    source_name, source_service, res = self._queue.get()
                    results += res

                self._ids_list[self._name_type[name]] = results
            else:
                results = self._ids_list[self._name_type[name]]

            for r, id in results:
                new_node = ExpressionTreeNode(tree_node.get_value(), tree_node.get_left_node(), ExpressionTreeNode(r))
                ids = self.fetch_ids(new_node)
                for i in ids:
                    if tree_node.get_right_node().get_value().split(".")[0] == self._node_name['node2']:
                        results_ids.append((i, id))
                    else:
                        results_ids.append((id, i))
            results_ids = list(set(results_ids))
        elif self.is_variable(tree_node.get_left_node().get_value()):
            ids = self.fetch_ids(tree_node)
            for name in self._name_type.keys():
                if name != tree_node.get_left_node().get_value().split(".")[0]:
                    if self._name_type[name] not in self._ids_list.keys():
                        find = FindAllIndex('id', None, self._name_type[name], None, "id")
                        results = []
                        for client in self._communication_service.get_client_list():
                            self.send(client.get_name(), 'IndexService', find)

                        for client in self._communication_service.get_client_list():
                            source_name, source_service, res = self._queue.get()
                            results += res
                        self._ids_list[self._name_type[name]] = results
                    else:
                        results = self._ids_list[self._name_type[name]]
            for id in ids:
                for r, x in results:
                    if tree_node.get_left_node().get_value().split(".")[0] == self._node_name['node1']:
                        results_ids.append((id, r))
                    else:
                        results_ids.append((r, id))
        elif self.is_variable(tree_node.get_right_node().get_value()):
            new_node = ExpressionTreeNode(Operator.get_opposite(tree_node.get_value()), tree_node.get_right_node(), tree_node.get_left_node())
            ids = self.fetch_ids(new_node)
            for name in self._name_type.keys():
                if name != tree_node.get_left_node().get_value().split(".")[0]:
                    if self._name_type[name] not in self._ids_list.keys():
                        find = FindAllIndex('id', None, self._name_type[name], None, "id")
                        results = []
                        for client in self._communication_service.get_client_list():
                            self.send(client.get_name(), 'IndexService', find)

                        for client in self._communication_service.get_client_list():
                            source_name, source_service, res = self._queue.get()
                            results += res
                        self._ids_list[self._name_type[name]] = results
                    else:
                        results = self._ids_list[self.self._name_type[name]]
            for id in ids:
                for r, x in results:
                    if tree_node.get_right_node().get_value().split(".")[0] == self._node_name['node1']:
                        results_ids.append((id, r))
                    else:
                        results_ids.append((r, id))

        return results_ids

    def get_results(self, tree_node):
        if tree_node.get_value() == '&&':
            results1 = self.get_results(tree_node.get_left_node())
            results2 = self.get_results(tree_node.get_right_node())
            results = self.common_results(results1, results2)
            return results
        elif tree_node.get_value() == '||':
            results1 = self.get_results(tree_node.get_left_node())
            results2 = self.get_results(tree_node.get_right_node())
            results = self.combine_results(results1, results2)
            return results
        else:
            return self.fetch_results(tree_node)


    def insert_edge_planner(self):
        query_graph = self._query.get_query_graph().get_node_edge_list()
        self._name_type = {}
        self._node_name = {}
        self._node_name['node1'] = query_graph[0]
        self._node_name['node2'] = query_graph[2]
        self._name_type[query_graph[0]] = query_graph[1]
        self._name_type[query_graph[2]] = query_graph[3]

        expression_tree = self._query.get_expression()
        results = self.get_results(expression_tree.get_root_node())
        return results

    def get_edge_end_ids(self, index_name, key, edge_type):
        if key in self._cache_edge_list.keys():
            return self._cache_edge_list[key]
        find_all_index = FindAllIndex(index_name, key, edge_type, Operator.equal_to, "value")
        results = []
        for client in self._communication_service.get_client_list():
            self.send(client.get_name(), 'IndexService', find_all_index)

        for client in self._communication_service.get_client_list():
            s_name, s_service, res = self._queue.get()
            results += res
        self._cache_edge_list[key] = results
        return results

    def intersection(self, a, b):
        return list(set.intersection(set(a), set(b)))

    def union(self, a, b):
        return list(set.union(set(a), set(b)))

    def fetch_query_results(self, expression_node):
        left_value = expression_node.get_left_node().get_value()
        right_value = expression_node.get_right_node().get_value()
        results_ids = []

        if self.is_variable(left_value) and self.is_variable(right_value):
            variable, attribute = right_value.split(".")
            find_all_index = FindAllIndex(attribute, None, self._name_type[variable], None, "id")
            results = []
            if self._name_type[variable] not in self._ids_list.keys():
                for client in self._communication_service.get_client_list():
                    self.send(client.get_name(), 'IndexService', find_all_index)

                for client in self._communication_service.get_client_list():
                    s_name, s_service, res = self._queue.get()
                    results += res
                self._ids_list[self._name_type[variable]] = results
            else:
                results = self._ids_list[self._name_type[variable]]

            for value, id in results:
                new_node = ExpressionTreeNode(expression_node.get_value(), expression_node.get_left_node(), ExpressionTreeNode(value))
                node_ids = self.fetch_ids(new_node)
                key = id
                if variable == self._node_name['node1']:
                    index_name = 'node_id1-id2'
                else:
                    index_name = 'node_id2-id1'
                edge_type = self._name_type[self._node_name['edge']]
                edge_ids = self.get_edge_end_ids(index_name, key, edge_type)

                ids = self.intersection(node_ids, edge_ids)
                for i in ids:
                    if variable == self._node_name['node2']:
                        results_ids.append((i, id))
                    else:
                        results_ids.append((id, i))
            result_set = ResultSet("n1n2", results_ids)
            return result_set

        elif self.is_variable(left_value):
            ids = self.fetch_ids(expression_node)
            variable, attribute = left_value.split(".")
            if variable == self._node_name['node1']:
                type = 'n1'
            else:
                type = 'n2'

            result_set = ResultSet(type, ids)
            return result_set

        elif self.is_variable(right_value):
            new_node = ExpressionTreeNode(Operator.get_opposite(expression_node.get_value()), expression_node.get_right_node(), expression_node.get_left_node())
            ids = self.fetch_ids(new_node)
            variable, attribute = right_value.split(".")
            if variable == self._node_name['node1']:
                type = 'n1'
            else:
                type = 'n2'

            result_set = ResultSet(type, ids)
            return result_set

    def swap(self, a, b):
        x = a
        a = b
        b = x
        return a,b

    # && condition
    def common_result_set(self, result_set1, result_set2):
        type1 = result_set1.get_type()
        type2 = result_set2.get_type()
        if type1 == type2:
            result_set = ResultSet(type1, self.intersection(result_set1.get_results(), result_set2.get_results()))
            return result_set

        if type1 == "n2" and type2 == "n1":
            result_set1, result_set2 = self.swap(result_set1, result_set2)
            type1, type2 = self.swap(type1, type2)

        if type1 == "n1n2" and type2 == "n1":
            result_set1, result_set2 = self.swap(result_set1, result_set2)
            type1, type2 = self.swap(type1, type2)

        if type1 == "n1n2" and type2 == "n2":
            result_set1, result_set2 = self.swap(result_set1, result_set2)
            type1, type2 = self.swap(type1, type2)

        if type1 == "n1":
            if type2 == "n2":
                result_ids = []
                for id in result_set1.get_results():
                    index_name = "node_id1-id2"
                    edge_type = self._name_type[self._node_name['edge']]
                    edge_ids = self.get_edge_end_ids(index_name, id, edge_type)

                    node_ids = self.intersection(result_set2.get_results(), edge_ids)
                    for node_id in node_ids:
                        result_ids.append((id, node_id))
                result_set = ResultSet("n1n2", result_ids)
                return result_set

            if type2 == "n1n2":
                result_ids = []
                for id1, id2 in result_set2.get_results():
                    if id1 in result_set1.get_results():
                        result_ids.append((id1, id2))
                result_set = ResultSet("n1n2", result_ids)
                return result_set

        if type1 == "n2":
            if type2 == "n1n2":
                result_ids = []
                for id1, id2 in result_set2.get_results():
                    if id2 in result_set1.get_results():
                        result_ids.append((id1, id2))
                result_set = ResultSet("n1n2", result_ids)
                return result_set

    # || condition
    def combine_result_set(self, result_set1, result_set2):
        type1 = result_set1.get_type()
        type2 = result_set2.get_type()
        if type1 == type2:
            result_set = ResultSet(type1, self.union(result_set1.get_results(), result_set2.get_results()))
            return result_set

        if type1 == "n2" and type2 == "n1":
            result_set1, result_set2 = self.swap(result_set1, result_set2)
            type1, type2 = self.swap(type1, type2)

        if type1 == "n1n2" and type2 == "n1":
            result_set1, result_set2 = self.swap(result_set1, result_set2)
            type1, type2 = self.swap(type1, type2)

        if type1 == "n1n2" and type2 == "n2":
            result_set1, result_set2 = self.swap(result_set1, result_set2)
            type1, type2 = self.swap(type1, type2)

        if type1 == "n1":
            if type2 == "n2":
                result1_ids = []
                result2_ids = []
                for id1 in result_set1.get_results():
                    index_name = "node_id1-id2"
                    edge_type = self._name_type[self._node_name['edge']]
                    node2_ids = self.get_edge_end_ids(index_name, id1, edge_type)
                    for node2_id in node2_ids:
                        result1_ids.append((id1, node2_id))

                for id2 in result_set2.get_results():
                    index_name = "node_id2-id1"
                    edge_type = self._name_type[self._node_name['edge']]
                    node1_ids = self.get_edge_end_ids(index_name, id2, edge_type)
                    for node1_id in node1_ids:
                        result2_ids.append((node1_id, id2))
                result_ids = self.union(result1_ids, result2_ids)
                result_set = ResultSet("n1n2", result_ids)
                return result_set

            if type2 == "n1n2":
                result1_ids = []
                for id1 in result_set1.get_results():
                    index_name = "node_id1-id2"
                    edge_type = self._name_type[self._node_name['edge']]
                    node2_ids = self.get_edge_end_ids(index_name, id1, edge_type)
                    for node2_id in node2_ids:
                        result1_ids.append((id1, node2_id))

                result_ids = self.union(result1_ids, result_set2.get_results())
                result_set = ResultSet("n1n2", result_ids)
                return result_set
        if type1 == "n2":
            if type2 == "n1n2":
                result2_ids = []

                for id2 in result_set1.get_results():
                    index_name = "node_id2-id1"
                    edge_type = self._name_type[self._node_name['edge']]
                    node1_ids = self.get_edge_end_ids(index_name, id2, edge_type)
                    for node1_id in node1_ids:
                        result2_ids.append((node1_id, id2))
                result_ids = self.union(result2_ids, result_set2.get_results())
                result_set = ResultSet("n1n2", result_ids)
                return result_set


    def get_query_results(self, expression_node):
        if expression_node.get_value() == '&&':
            result_set1 = self.get_query_results(expression_node.get_left_node())
            result_set2 = self.get_query_results(expression_node.get_right_node())
            result_set = self.common_result_set(result_set1, result_set2)
            return result_set
        elif expression_node.get_value() == '||':
            result_set1 = self.get_query_results(expression_node.get_left_node())
            result_set2 = self.get_query_results(expression_node.get_right_node())
            result_set = self.combine_result_set(result_set1, result_set2)
            return result_set
        else:
            result_set = self.fetch_query_results(expression_node)
            return result_set

    def get_rows(self, result_set):
        r = ''
        results = []
        properties = self._query.get_return_variables()
        row = []
        for p in properties:
            r += "%-35s" %(p)
            row.append(p)
        results.append(row)
        r += '\n'
        for id1, id2 in result_set.get_results():
            if id1 not in self._cache_nodes.keys():
                find_node = FindNode(id1, self._name_type[self._node_name['node1']])
                for client in self._communication_service.get_client_list():
                    self.send(client.get_name(), 'StorageService', find_node)
                Node1 = None
                for client in self._communication_service.get_client_list():
                    s_name, s_service, node1 = self._queue.get()
                    if node1 is not None:
                        Node1 = node1

                if Node1 is None:
                    continue
                node1 = Node1
                self._cache_nodes[id1] = node1
            else:
                node1 = self._cache_nodes[id1]

            if id2 not in self._cache_nodes.keys():
                find_node = FindNode(id2, self._name_type[self._node_name['node2']])
                for client in self._communication_service.get_client_list():
                    self.send(client.get_name(), 'StorageService', find_node)
                Node2 = None
                for client in self._communication_service.get_client_list():
                    s_name, s_service, node2 = self._queue.get()
                    if node2 is not None:
                        Node2 = node2

                if Node2 is None:
                    continue
                node2 = Node2
                self._cache_nodes[id2] = node2
            else:
                node2 = self._cache_nodes[id2]
            row = []
            for p in properties:
                var, attrib = p.split(".")
                if self._node_name['node1'] == var:
                    r += "%-35s" %(str(node1.get_property(attrib)))
                    row.append(str(node1.get_property(attrib)))
                else:
                    r += "%-35s" %(str(node2.get_property(attrib)))
                    row.append(str(node2.get_property(attrib)))
            results.append(row)
            r += '\n'
        return results


    def get_result_data_from_set(self, result_set):
        if result_set.get_type() == 'n1':
            result = []
            for id in result_set.get_results():
                index_name = "node_id1-id2"
                edge_type = self._name_type[self._node_name['edge']]
                edge_ids = self.get_edge_end_ids(index_name, id, edge_type)

                for eid in edge_ids:
                    result.append((id, eid))

            res_set = ResultSet('n1n2', result)
            result = self.get_rows(res_set)

        if result_set.get_type() == 'n2':
            result = []
            for id in result_set.get_results():
                index_name = "node_id2-id1"
                edge_type = self._name_type[self._node_name['edge']]
                edge_ids = self.get_edge_end_ids(index_name, id, edge_type)
                for eid in edge_ids:
                    result.append((eid, id))
            res_set = ResultSet('n1n2', result)
            result = self.get_rows(res_set)

        if result_set.get_type() == 'n1n2':
            result = self.get_rows(result_set)

        return result

    def get_node_list(self, result_set):
        r = ''
        results = []
        row = []
        properties = self._query.get_return_variables()
        for p in properties:
            r += "%-35s" %(p)
            row.append(p)

        results.append(row)
        r += '\n'

        for id in result_set.get_results():
            if id not in self._cache_nodes.keys():
                find_node = FindNode(id, self._name_type[self._node_name['node1']])
                for client in self._communication_service.get_client_list():
                    self.send(client.get_name(), 'StorageService', find_node)
                Node1 = None
                for client in self._communication_service.get_client_list():
                    s_name, s_service, node1 = self._queue.get()
                    if node1 is not None:
                        Node1 = node1
                if Node1 is None:
                    continue
                node1 = Node1
                self._cache_nodes[id] = node1
            else:
                node1 = self._cache_nodes[id]
            row = []
            for p in properties:
                var, attrib = p.split(".")
                if self._node_name['node1'] == var:
                    r += "%-35s" %(str(node1.get_property(attrib)))
                    row.append(str(node1.get_property(attrib)))
            results.append(row)

            r += '\n'
        return results
    def get_all_results(self):
        var = self._node_name['node1']
        find = FindAllIndex('id', None, self._name_type[var], None, "value")
        res = []
        for client in self._communication_service.get_client_list():
            self.send(client.get_name(), 'IndexService', find)

        for client in self._communication_service.get_client_list():
            source_name, source_service, results = self._queue.get()
            for id, value in results:
                res.append(id)
        result_set = ResultSet('n1', res)
        return result_set

    def execution_planner(self):
        self.set_basic_info_from_graph()
        expression_tree = self._query.get_expression()

        if expression_tree is None:
            result_set = self.get_all_results()
        else:
            result_set = self.get_query_results(expression_tree.get_root_node())
        if len(self._query.get_query_graph().get_node_edge_list()) == 2:
            result = self.get_node_list(result_set)
        else:
            result = self.get_result_data_from_set(result_set)
        return result







