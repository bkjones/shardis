from itertools import chain
from zlib import crc32
from redis import Connection, ConnectionPool

__author__ = 'Brian K. Jones'
__email__ = 'bkjones@gmail.com'
__since__ = '2011-12-05'

class ShardPool(ConnectionPool):
    """
    A redis.ConnectionPool class that supports sharding in a way that's
    transparent to the end developer.
    """
    def __init__(self, servers, hash_type='default',
                 connection_class=Connection, max_connections=None,
                 **connection_kwargs):

        self.connection_class = connection_class
        self.max_connections = max_connections or 2**31
        self._available_connections = dict()
        self._in_use_connections = dict()
        self._created_connections = 0
        self.hash_func = hash_type

        self.servers = servers

        # build nodes, named using host:port of each server. So
        # sharding across ports on a single host is possible.
        self.nodes = list()
        for server in servers:
            newnode = dict()
            newnode['name'] = ':'.join([server['host'], str(server['port'])])
            newnode['dsn'] = server
            self.nodes.append(newnode)
            self._available_connections[newnode['name']] = list()
            self._in_use_connections[newnode['name']] = set()

    def get_node_offset(self, key):
        """
        The hash function used to find the index into the
        list of nodes that will be used.

        """
        c = crc32(key) >> 16 & 0x7fff
        return c % len(self.nodes)

    def get_connection(self, command_name, *keys, **options):
        """Get a connection from the pool"""
        offset = self.get_node_offset(keys[0])
        node = self.nodes[offset]
        try:
            connection = self._available_connections[node['name']].pop()
        except IndexError:
            # pop'd an empty list. Need a new connection.
            connection = self.make_connection(node['dsn'])
        self._in_use_connections[node['name']].add(connection)
        return connection

    def make_connection(self, server):
        """Create a new connection"""
        if self._created_connections >= self.max_connections:
            raise ConnectionError("Too many connections")
            # this comes from base redis module. Not sure it makes a lot
        # of sense to have this.
        self._created_connections += 1
        return self.connection_class(**server)

    def release(self, connection):
        """Releases the connection back to the pool. This is, unfortunately,
        called directly by StrictRedis, which doesn't know about our whole
        sharding scheme. It shouldn't know about the connection or connection
        pool either. Alas, we press on and formulate a nodename here manually
        from the connection parts :/

        """
        nodename = ':'.join([connection.host, str(connection.port)])
        self._in_use_connections[nodename].remove(connection)
        self._available_connections[nodename].append(connection)

    def disconnect(self):
        """Disconnects all connections in the pool"""
        all_conns = chain(self._available_connections, self._in_use_connections)
        for connection in all_conns:
            connection.disconnect()

