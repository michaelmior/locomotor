import ast
import datetime
import imp
import pytest
import time

from locomotor import redis_server

@pytest.fixture(scope='session')
def redis(request):
    import redis
    client = redis.StrictRedis()

    # Properly drop the database when done
    def fin():
        client.script_flush()
        client.flushall()
    request.addfinalizer(fin)

    return client

def test_get_by_category(redis):
    def get_by_category(client, category):
        ids = client.lrange('category:' + category, 0, -1)
        items = []
        for id in ids:
            items.append(client.hget(id, 'name'))
        return items

    redis.hmset('item:1', { 'name': 'Foo', 'category': 'Bar' })
    redis.lpush('category:Bar', 'item:1')

    assert get_by_category(redis, 'Bar') == ['Foo']

def test_add_link(redis):
    @redis_server
    def add_link(client, url):
        link_id = client.incr('counter')
        client.hset('links', link_id, url)
        return link_id

    assert add_link(redis, 'foo') == 1
    assert add_link(redis, 'bar') == 2
    assert add_link.__name__ == 'add_link'

def test_increx(redis):
    class Foo:
        KEY_EXISTS = 1

        @redis_server
        def increx(self, client, key):
            if client.exists(key) == self.KEY_EXISTS:
                return client.incr(key)

    assert Foo().increx(redis, 'fooincr') == None
    redis.set('fooincr', 1)
    assert Foo().increx(redis, 'fooincr') == 2

def test_type(redis):
    def return_value(client, value):
        return value

    redis_objs=['client']
    assert redis_server(return_value, redis_objs=redis_objs)(redis, 3) == 3
    assert redis_server(return_value, redis_objs=redis_objs)(redis, 'foo') \
            == 'foo'
    assert redis_server(return_value, redis_objs=redis_objs)(redis, 2.71828) \
            == 2.71828

def test_loop(redis):
    @redis_server(redis_objs=['client'])
    def loop(client):
        items = []
        for i in range(10):
            items.append(i)
        return items

    assert loop(redis) == range(10)

def test_nil(redis):
    @redis_server(redis_objs=['client'])
    def nil(client):
        return None

    assert nil(redis) == None

def test_nil_constant(redis):
    class Foo:
        POTATO = None

        @redis_server(redis_objs=['client'])
        def nil(self, client):
            return self.POTATO

    assert Foo().nil(redis) == None

def test_function(redis):
    class Foo:
        KEY_SUFFIX = '1'

        def decorate_key(self, key):
            return key + self.KEY_SUFFIX

        @redis_server(redis_objs=['client'])
        def get_decorated(self, client, key):
            return client.get(self.decorate_key(key))

    redis.set('foofunc1', 'bar')
    assert Foo().get_decorated(redis, 'foofunc') == 'bar'
    assert Foo().get_decorated.__name__ == 'get_decorated'

def test_shard(redis):
    class Foo:
        db_count = 3

        def shard(self, w_id) :
            return int(w_id) % self.db_count

        @redis_server(redis_objs=['client'])
        def get(self, client):
            return self.shard('10')

    assert Foo().get(redis) == 1

def test_array(redis):
    @redis_server(redis_objs=['client'])
    def foo(client, array):
        return array[0]

    assert foo(redis, [3, 2, 1]) == 3

def test_dict(redis):
    @redis_server(redis_objs=['client'])
    def foo(client, d, k):
        return d[k]

    assert foo(redis, {'a': 1, 'b': 2}, 'b') == 2

def test_dict_literal(redis):
    @redis_server(redis_objs=['client'])
    def foo(client):
        return {'a': 1}['a']

    assert foo(redis) == 1

def test_execute(redis):
    @redis_server(redis_objs=['client'])
    def pipe(client, key1, key2):
        client.pipe()
        client.get(key1)
        client.get(key2)
        values = client.execute()

        return values[0] + values[1]

    redis.set('pipe_foo', 'baz')
    redis.set('pipe_bar', 'quux')
    assert pipe(redis, 'pipe_foo', 'pipe_bar') == 'bazquux'

def test_insert(redis):
    @redis_server(redis_objs=['client'])
    def insert(client):
        x = []
        x.insert(0, 1)
        return x[0]

    assert insert(redis) == 1

def test_constant(redis):
    FOO = 3

    @redis_server(redis_objs=['client'])
    def constant(client):
        return FOO

    assert constant(redis) == 3

def test_class_constant(redis):
    class constants:
        FOO = 3

    @redis_server(redis_objs=['client'])
    def constant(client):
        return constants.FOO

    assert constant(redis) == 3

def test_nested_func(redis):
    class constants:
        STRING = 'foo'

    @redis_server(redis_objs=['client'])
    def replace(client):
        return constants.STRING.replace('foo', 'bar')

    assert replace(redis) == 'bar'

def test_string_replace(redis):
    @redis_server(redis_objs=['client'])
    def replace(client, string):
        return string.replace('foo', 'bar')

    assert replace(redis, 'foo') == 'bar'

def test_string_join(redis):
    @redis_server(redis_objs=['client'])
    def join(client, arr):
        return ', '.join(arr)

    assert join(redis, ['a', 'b', 'c']) == 'a, b, c'

def test_newline(redis):
    @redis_server(redis_objs=['client'])
    def newline(client):
        return '\n'

    assert newline(redis) == '\n'

def test_usub(redis):
    @redis_server(redis_objs=['client'])
    def usub(client, val):
        return -val

    assert usub(redis, 3) == -3

def test_tpcc_fragment(redis):
    constants = imp.load_source('constants',
                                'vendor/pytpcc/pytpcc/constants.py')

    class RedisDriver:
        KEY_SEPARATOR = ':'

        def safeKey(self, keys) :
                new_keys = []
                for k in keys :
                    new_keys.append(str(k))
                return self.KEY_SEPARATOR.join(new_keys
                    ).replace('\n', '').replace(' ','')

        @redis_server
        def doDelivery(self, rdr):
            # Initialize input parameters
            w_id = o_carrier_id = ol_delivery_d = '1'

            #-------------------------
            # Initialize Data Holders
            #-------------------------
            order_key = [ ]
            ol_total = [ ]
            customer_key = [ ]
            ol_counts = [ ]
            no_o_id = [ ]
            for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1) :
                order_key.append(None)
                ol_total.append(0)
                customer_key.append(None)
                ol_counts.append(0)

            #---------------------
            # Get New Order Query
            #---------------------
            for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1) :
                cursor = d_id - 1
                # Get set of possible new order ids
                index_key = self.safeKey([d_id, w_id])
                rdr.srandmember('NEW_ORDER.INDEXES.GETNEWORDER.' + index_key)
            id_set = rdr.execute()

            for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1) :
                cursor = d_id - 1
                if id_set[cursor] == None :
                    rdr.get('NULL_VALUE')
                else :
                    rdr.hget('NEW_ORDER.' + str(id_set[cursor]), 'NO_O_ID')
            no_o_id = rdr.execute()

            return None

    assert RedisDriver().doDelivery(redis) == None

def test_multiply(redis):
    @redis_server(redis_objs=['client'])
    def multiply(client, m, n):
        return m * n

    assert multiply(redis, 3, 4) == 12

def test_divide(redis):
    @redis_server(redis_objs=['client'])
    def divide(client, m, n):
        return m / n

    assert divide(redis, 12, 4) == 3

def test_power(redis):
    @redis_server(redis_objs=['client'])
    def power(client, m, n):
        return m ** n

    assert power(redis, 3, 3) == 27

def test_or(redis):
    @redis_server(redis_objs=['client'])
    def bool_or(client):
        return True or False

    assert bool_or(redis)

def test_and(redis):
    @redis_server(redis_objs=['client'])
    def bool_and(client):
        return True and False

    assert not bool_and(redis)

def test_partial(redis, capfd):
    @redis_server(redis_objs=['client'], minlineno=3, maxlineno=3)
    def partial(client):
        print('LOCAL')
        return client.get('foo')

    redis.set('foo', 'bar')

    assert partial(redis) == 'bar'

    out, _ = capfd.readouterr()
    assert out == 'LOCAL\n'

def test_pass(redis):
    @redis_server(redis_objs=['client'])
    def pass_func(client):
        pass
        return 3

    assert pass_func(redis) == 3

def test_compare(redis):
    @redis_server(redis_objs=['client'])
    def compare(client):
        if 3 < 4:
            return 2 > 1
        else:
            return 2 >= 5 and 2 != 3 and 5 <= 7

    assert compare(redis) == True

def test_augassign(redis):
    @redis_server(redis_objs=['client'])
    def augassign(client):
        x = 1
        x += 2
        return x

    assert augassign(redis) == 3

def test_tuple(redis):
    @redis_server(redis_objs=['client'])
    def tuple(client):
        return (2, 3)

    assert tuple(redis) == [2, 3]

def test_continue(redis):
    @redis_server(redis_objs=['client'])
    def cont(client):
        for i in range(10):
            continue
            return False

        return True

    assert cont(redis) == True

def test_break(redis):
    @redis_server(redis_objs=['client'])
    def brk(client):
        for i in range(10):
            break
            return False

        return True

    assert brk(redis) == True

def test_pipe(redis):
    @redis_server(redis_objs=['client'])
    def pipe(client):
        return 1337

    redis = redis.pipeline()
    assert pipe(redis) == 1337

def test_reassign(redis):
    @redis_server(redis_objs=['client'])
    def reassign(client):
        x = 3
        x = 4
        return x

    assert reassign(redis) == 4

def test_local_scope(redis):
    @redis_server(redis_objs=['client'])
    def local_scope(client):
        if True:
            x = 3
        else:
            x = 4
        return x

    assert local_scope(redis) == 3

def test_delete(redis):
    @redis_server(redis_objs=['client'])
    def delete(client):
        client.delete('foo')

    redis.set('foo', 'bar')
    delete(redis)
    assert redis.get('foo') == None

def test_datetime(redis):
    @redis_server(redis_objs=['client'])
    def convert_datetime(client, dt):
        return dt

    # XXX We do not properly round trip datetime objects yet
    assert convert_datetime(redis, datetime.datetime.now()) is not None

def test_unary(redis):
    @redis_server(redis_objs=['client'])
    def unary(client):
        return +3

    assert unary(redis) == 3

def test_array_len(redis):
    @redis_server(redis_objs=['client'])
    def array_len(client):
        return len([1, 2, 3])

    assert array_len(redis) == 3

def test_string_len(redis):
    @redis_server(redis_objs=['client'])
    def string_len(client):
        return len('foo')

    assert string_len(redis) == 3

def test_multiple_objs(redis):
    @redis_server(redis_objs=['client1', 'client2'])
    def multiple_objs(client1, client2, value):
        client1.set('foo', value)
        return client2.get('foo')

    assert multiple_objs(redis, redis, 'bar') == 'bar'

def test_time(redis):
    @redis_server(redis_objs=['client'])
    def lua_time(client):
        return time.time()

    assert abs(lua_time(redis) - time.time()) < 1

def test_array_not(redis):
    @redis_server(redis_objs=['client'])
    def array_not(client):
        return not []

    assert array_not(redis)

def test_string_not(redis):
    @redis_server(redis_objs=['client'])
    def string_not(client):
        return not ''

    assert string_not(redis)

def test_number_not(redis):
    @redis_server(redis_objs=['client'])
    def number_not(client):
        return not 0

    assert number_not(redis)

def test_assign_array(redis):
    @redis_server(redis_objs=['client'])
    def assign_array(client):
        x = [1, 2, 3]
        x[0] = 4
        return x[0]

    assert assign_array(redis) == 4

def test_assign_dict(redis):
    @redis_server(redis_objs=['client'])
    def assign_dict(client):
        x = {'a': 1}
        x['a'] = 2
        return x['a']

    assert assign_dict(redis) == 2
