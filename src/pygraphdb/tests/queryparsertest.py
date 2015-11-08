__author__ = 'Sagar'
import unittest
from pygraphdb.services.master.queryparser import QueryParser, InsertNodeQuery, FindNodeQuery

class QueryParserTest(unittest.TestCase):
    def setUp(self):
        self._qp = QueryParser()

    def tearDown(self):
        pass

    def test_query(self):
        query = "insert Node Person { name:'Sagar Chakravarthy', age:21 }"
        #self._qp.parse(query)

        query = "find moviea:Movie -movieedge:MovieEdge> movieb:Movie where moviea.name == 'Inception' || movieb.year >= 2012 return movieb.name"
        print(query[64:])
        obj = self._qp.parse(query)
        obj.display()

if __name__ == '__main__':
    unittest.main()