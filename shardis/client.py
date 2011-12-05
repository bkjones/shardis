"""
Prototype testing a redis.ConnectionPool subclass that supports sharding in
a way that's transparent to the end developer.

"""
from redis import StrictRedis
from .pool import ShardPool

__author__ = 'Brian K. Jones'
__email__ = 'bkjones@gmail.com'
__since__ = '2011-12-05'


class ShardClient(StrictRedis):
    def execute_command(self, *args, **options):
        """Execute a command and return a parsed response"""
        pool = self.connection_pool
        command_name = args[0]
        connection = pool.get_connection(command_name, *args[1:], **options)
        try:
            connection.send_command(*args)
            return self.parse_response(connection, command_name, **options)
        except ConnectionError:
            connection.disconnect()
            connection.send_command(*args)
            return self.parse_response(connection, command_name, **options)
        finally:
            pool.release(connection)


if __name__ == '__main__':
    servers = [{'host': 'localhost', 'port': 6379, 'db': 0},
               {'host': 'localhost', 'port': 6380, 'db': 0}]
    p = ShardPool(servers=servers)
    c = ShardClient(connection_pool=p)
    c.set('whatever', 'foobar')
    c.set('blah', 'blaz')
    c.set('1234asdfzxv', 'fje,eodk')
    v1 = c.get('1234asdfzxv')
    v2 = c.get('blah')
    print "v1: %s" % v1
    print "v2: %s" % v2
    assert v1 == 'fje,eodk'
    assert v2 == 'blaz'
