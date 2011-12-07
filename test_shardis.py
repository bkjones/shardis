#!/usr/bin/env python
import unittest2 as unittest
from mock import Mock, patch
from shardis import pool, client
from redis import ConnectionError

class TestPool(unittest.TestCase):
    def setUp(self):
        self.servers =[{'host': 'foo', 'port': 9, 'db': 0},
                {'host': 'bar', 'port': 8, 'db': 1}]

    def test_pool_exists(self):
        self.assertIn('ShardPool', dir(pool))

    def test_pool_init_expected_case(self):

        # this depends on self.servers never changing :/
        expected_nodes = [{'name': 'foo:9', 'dsn': self.servers[0]},
                          {'name': 'bar:8', 'dsn': self.servers[1]}]
        mypool = pool.ShardPool(servers=self.servers)
        self.assertIsInstance(mypool, pool.ShardPool)
        self.assertEqual(mypool.max_connections, 2**31)
        self.assertFalse(mypool._created_connections)
        self.assertEqual(mypool.servers, self.servers)
        self.assertEqual(mypool.nodes, expected_nodes)

    def test_get_node_offset(self):
        mypool = pool.ShardPool(self.servers)
        vals = [' ', '  ', '   ', 'a', '1', '0', 'abc', ',c,d', '94.5']
        offsets = [mypool.get_node_offset(x) for x in vals]
        self.assertFalse(any([i > 1 or i < 0 for i in offsets]))

    def test_get_connection_calls_make_conn(self):
        """
        If there are no available connections, get_connection should call
        make_connection, passing a server dict used to make the connection.
        :return:
        """
        mypool = pool.ShardPool(self.servers)
        with patch('shardis.pool.ShardPool.make_connection') as fake_makeconn:
            mypool.get_connection('GET', ' ') # offset of empty string is 0
        fake_makeconn.assert_called_once_with(self.servers[0])

    def test_get_connection_incrs_created(self):
        mypool = pool.ShardPool(self.servers)
        mypool.connection_class = Mock()
        self.assertFalse(mypool._created_connections)
        mypool.make_connection(self.servers[0])
        self.assertEqual(mypool._created_connections, 1)

    def test_make_conn_too_many_conns(self):
        mypool = pool.ShardPool(self.servers)
        mypool.max_connections = 1
        mypool._created_connections = 2
        mypool.connection_class = Mock()
        with self.assertRaises(ConnectionError):
            mypool.make_connection(self.servers[0])

    def test_release_connection(self):
        conn = Mock()
        conn.host = 'foo'
        conn.port = 9
        mypool = pool.ShardPool(self.servers)
        mypool._in_use_connections['foo:9'].add(conn)
        self.assertTrue(mypool._in_use_connections['foo:9'])
        mypool.release(conn)
        self.assertFalse(mypool._in_use_connections['foo:9'])

    def test_disconnect_noconns(self):
        mypool = pool.ShardPool(self.servers)
        self.assertIsNone(mypool.disconnect())



class TestClient(unittest.TestCase):
    pass
