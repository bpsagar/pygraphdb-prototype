__author__ = 'Sagar'
from pyparsing import Literal, Word, alphas, alphanums, nums, Suppress, Optional, OneOrMore, quotedString, ZeroOrMore, CaselessLiteral, QuotedString, Forward, Group
from threading import Lock
from pygraphdb.services.master.expressiontreenode import ExpressionTree

class QueryParser(object):
    def __init__(self):
        super(QueryParser, self).__init__()
        self._query_obj = None

    def set_type(self, orig_string, loc, tokens):
        node_type = tokens[0]
        self._query_obj.set_type(node_type)

    def create_insert_node_query_obj(self, orig_string, loc, tokens):
        self._query_obj = InsertNodeQuery()

    def set_property(self, orig_string, loc, tokens):
        name = tokens[0]
        value = tokens[1]
        self._query_obj.set_property(name, value)

    def create_find_node_query_obj(self, orig_string, loc, tokens):
        self._query_obj = FindNodeQuery()

    def set_query_graph(self, orig_string, loc, tokens):
        graph = []
        graph = [token for token in tokens]
        query_graph = QueryGraph()
        query_graph.set_values(graph)
        self._query_obj.set_query_obj(query_graph)

    def set_expression(self, orig_string, loc, tokens):
        exp = []
        for val in tokens:
            exp.append(val)
        expression = ExpressionTree(exp)
        expression.initialise()
        self._query_obj.set_expression(expression)

    def set_variable_list(self, orig_string, loc, tokens):
        var = []
        for val in tokens:
            var.append(val)
        self._query_obj.set_variable_list(var)

    def convert_to_int(self, orig_string, loc, tokens):
        return int(tokens[0])

    def create_new_type_query_obj(self, orig, loc, tokens):
        self._query_obj = CreateTypeQuery()

    def set_parent_type(self, orig_string, loc, tokens):
        self._query_obj.set_parent_type(tokens[0])

    def set_description(self, orig_string, loc, tokens):
        self._query_obj.set_description(tokens[0])

    def create_delete_type_query_obj(self, orig_string, loc, tokens):
        self._query_obj = DeleteTypeQuery()

    def set_batch(self):
        self._query_obj.set_batch(True)

    def create_type_query_syntax(self):
        create_type_keyword = CaselessLiteral("CREATE TYPE").setParseAction(self.create_new_type_query_obj)
        new_type = Word(alphas).setParseAction(self.set_type)
        parent_type = Word(alphas).setParseAction(self.set_parent_type)
        description = QuotedString("'",multiline=True) ^ QuotedString('"',multiline=True)
        description.setParseAction(self.set_description)
        create_type_query = create_type_keyword + new_type + parent_type + description
        return create_type_query

    def delete_type_query_syntax(self):
        delete_type_keyword = CaselessLiteral("DELETE TYPE").setParseAction(self.create_delete_type_query_obj)
        delete_type = Word(alphas).setParseAction(self.set_type)
        delete_type_query = delete_type_keyword + delete_type
        return delete_type_query

    def insert_node_query_syntax(self):
        insert_node_keyword = CaselessLiteral("INSERT NODE").setParseAction(self.create_insert_node_query_obj)
        type_name = Word(alphas).setParseAction(self.set_type)
        name = Word(alphas)
        string = QuotedString("'",multiline=True) ^ QuotedString('"',multiline=True)
        value = string ^ (Word(nums).setParseAction(self.convert_to_int))
        name_value = name + Suppress(Literal(":")) + value + Suppress(Optional(Literal(',')))
        name_value.setParseAction(self.set_property)
        batch = Literal("-b").setParseAction(self.set_batch)
        insert_node_query = insert_node_keyword + type_name + Literal('{') + OneOrMore(name_value) + Literal('}') + Optional(batch)
        return insert_node_query

    def condition_expression_syntax(self):
        ops = ['==', '>', '<', '>=', '<=']
        cds = ['&&', '||']
        lpar = Literal('(').suppress()
        rpar = Literal(')').suppress()
        operators = Literal("==")
        conditions = Literal('&&')
        for cd in cds[1:]:
            conditions = conditions ^ Literal(cd)
        for op in ops[1:]:
            operators = operators ^ Literal(op)
        variable_name = Word(alphas)
        #variable = variable_name + Literal('.') + variable_name
        variable = Word(alphanums + ".")
        #variable.setParseAction(set_type_variable)
        constant = Word(nums) ^ QuotedString("'",multiline=True) ^ QuotedString('"',multiline=True)
        #constant.setParseAction(set_type_constant)
        operands = constant ^ variable
        expression = (operands + operators + operands)
        cexpr = Forward()
        atom = expression ^ ( Group(lpar + cexpr + rpar) )
        cexpr << atom + ZeroOrMore( conditions + cexpr)
        return cexpr

    def find_query_syntax(self):
        operators = "+-*/%=&|><!"
        find_keyword = CaselessLiteral("FIND").setParseAction(self.create_find_node_query_obj)
        variable = Word(alphanums)
        vtype = Word(alphanums)
        node = Optional(variable, default=None) + Suppress(Literal(":")) + vtype
        edge = Optional(variable, default=None) + Suppress(Literal(":")) + Optional(vtype, default=None)
        query_graph = node + ZeroOrMore(Suppress(Literal("-")) + edge + Suppress(Literal(">")) + node)
        query_graph.setParseAction(self.set_query_graph)
        where_keyword = CaselessLiteral("WHERE")
        expression = self.condition_expression_syntax()
        expression.setParseAction(self.set_expression)
        return_keyword = CaselessLiteral("RETURN")
        variable_list = Word(alphanums + '.') + ZeroOrMore(Suppress(Literal(',')) + Word(alphanums + '.'))
        variable_list.setParseAction(self.set_variable_list)
        find_node_query = find_keyword + OneOrMore(query_graph) + Optional(where_keyword + expression) + return_keyword + variable_list
        return find_node_query

    def create_insert_edge_query_obj(self):
        self._query_obj = InsertEdgeQuery()

    def insert_edge_query_syntax(self):
        insert_edge_keyword = CaselessLiteral("INSERT EDGE").setParseAction(self.create_insert_edge_query_obj)
        type_name = Word(alphas).setParseAction(self.set_type)
        variable = Word(alphanums)
        vtype = Word(alphanums)
        node = Optional(variable, default=None) + Suppress(Literal(":")) + vtype
        query_graph = node + Suppress(Literal(",")) + node
        query_graph.setParseAction(self.set_query_graph)
        where_keyword = CaselessLiteral("WHERE")
        expression = self.condition_expression_syntax()
        expression.setParseAction(self.set_expression)
        insert_edge_query = insert_edge_keyword + type_name + query_graph + where_keyword + expression
        return insert_edge_query

    def parse(self, query_string):
        #Grammar for the query parser

        #create type CREATE TYPE type parent_type "description"
        create_type_query = self.create_type_query_syntax()

        #delete type DELETE TYPE type
        delete_type_query = self.delete_type_query_syntax()

        #insert query INSERT NODE type { properties }
        insert_node_query = self.insert_node_query_syntax()

        # find query
        find_node_query = self.find_query_syntax()

        # insert edge node ,node where condition
        insert_edge_query = self.insert_edge_query_syntax()

        queries = find_node_query ^ insert_node_query ^ create_type_query ^ delete_type_query ^ insert_edge_query
        query_parse = queries.parseString(query_string)
        #self._query_obj.display()
        return self._query_obj

class Query(object):
    query_names = {}
    lock = Lock()

    @classmethod
    def get_unique_query_id(cls, obj):
        Query.lock.acquire()
        count = Query.query_names.get(obj.__class__, -1)
        count += 1
        Query.query_names[obj.__class__] = count
        Query.lock.release()
        return '%s-%d' % (obj.__class__.__name__, count)

    def __init__(self):
        self._query_id = Query.get_unique_query_id(self)

    def get_query_id(self):
        return self._query_id

class InsertNodeQuery(Query):
    def __init__(self):
        super(InsertNodeQuery, self).__init__()
        self._type = None
        self._batch = False
        self._properties = {}

    def get_properties(self):
        return self._properties

    def get_type(self):
        return self._type

    def set_property(self, name, value):
        self._properties[name] = value

    def set_type(self, node_type):
        self._type = node_type

    def set_batch(self, batch):
        self._batch = batch

    def get_batch(self):
        return self._batch

    def display(self):
        print("Type: ", self._type)

        for key in self._properties.keys():
            print(key, ":", self._properties[key])

class InsertEdgeQuery(Query):
    def __init__(self):
        super(InsertEdgeQuery, self).__init__()
        self._query_graph = None
        self._where_expression = []
        self._type = None

    def set_query_obj(self, query_obj):
        self._query_graph = query_obj

    def set_expression(self, expression):
        self._where_expression = expression

    def set_type(self, type):
        self._type = type

    def get_type(self):
        return self._type

    def get_query_graph(self):
        return self._query_graph

    def get_expression(self):
        return self._where_expression

    def display(self):
        print("Query Graph:")
        self._query_graph.display()

        print("Where :")
        self._where_expression.display()


class FindNodeQuery(Query):
    def __init__(self):
        super(FindNodeQuery, self).__init__()
        self._query_graph = None
        self._where_expression = None
        self._return_variable = []

    def set_query_obj(self, query_obj):
        self._query_graph = query_obj

    def set_expression(self, expression):
        self._where_expression = expression

    def set_variable_list(self, variable_list):
        self._return_variable = variable_list

    def get_query_graph(self):
        return self._query_graph

    def get_expression(self):
        return self._where_expression

    def get_return_variables(self):
        return self._return_variable

    def display(self):
        print("Query Graph:")
        self._query_graph.display()

        print("Where :")
        self._where_expression.display()
        print("Return :", self._return_variable)

class QueryGraph(object):
    def __init__(self):
        super(QueryGraph, self).__init__()
        self._node_edge_list = []

    def set_values(self, values):
        self._node_edge_list = values

    def display(self):
        print(self._node_edge_list)

    def get_node_edge_list(self):
        return self._node_edge_list

class CreateTypeQuery(Query):
    def __init__(self):
        super(CreateTypeQuery, self).__init__()
        self._type = None
        self._parent_type = None
        self._description = None

    def get_type(self):
        return self._type

    def get_parent_type(self):
        return self._parent_type

    def get_description(self):
        return self._description

    def set_type(self, type):
        self._type = type

    def set_parent_type(self, parent):
        self._parent_type = parent

    def set_description(self, description):
        self._description = description

    def display(self):
        print("Type: ", self._type)
        print("Parent type: ", self._parent_type)
        print("Description: ", self._description)

class DeleteTypeQuery(Query):
    def __init__(self):
        super(DeleteTypeQuery, self).__init__()
        self._type = None

    def get_type(self):
        return self._type

    def set_type(self, type):
        self._type = type
