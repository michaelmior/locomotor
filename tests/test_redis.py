import ast
import imp
import pytest

from querybench import redis_server

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

    assert get_by_category(redis, 'Bar') == \
            redis_server(get_by_category)(redis, 'Bar')

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

    assert redis_server(return_value)(redis, 3) == 3
    assert redis_server(return_value)(redis, 'foo') == 'foo'

    # XXX This currently fails since we can't directly return floating
    #     point numbers from Lua without string conversion and we only
    #     know to do a string conversion if we do type inference on the
    #     return value
    # assert redis_server(return_value)(redis, 2.71828) == 2.71828

def test_loop(redis):
    @redis_server
    def loop(client):
        items = []
        for i in range(10):
            items.append(i)
        return items

    assert loop(redis) == range(10)

def test_nil(redis):
    @redis_server
    def nil(client):
        return None

    assert nil(redis) == None

def test_function(redis):
    class Foo:
        KEY_SUFFIX = '1'

        def decorate_key(self, key):
            return key + self.KEY_SUFFIX

        @redis_server(redis_objs=[ast.Name(id='client', ctx=ast.Load())])
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

        @redis_server
        def get(self, client):
            return self.shard('10')

    assert Foo().get(redis) == 1

def test_array(redis):
    @redis_server
    def foo(client, array):
        return array[0]

    assert foo(redis, [3, 2, 1]) == 3

def test_dict(redis):
    @redis_server
    def foo(client, d, k):
        return d[k]

    assert foo(redis, {'a': 1, 'b': 2}, 'b') == 2

def test_execute(redis):
    @redis_server(redis_objs=[ast.Name(id='client', ctx=ast.Load())])
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
    @redis_server
    def insert(client):
        x = []
        x.insert(0, 1)
        return x[0]

    assert insert(redis) == 1

def test_constant(redis):
    FOO = 3

    @redis_server
    def constant(client):
        return FOO

    assert constant(redis) == 3

def test_class_constant(redis):
    class constants:
        FOO = 3

    @redis_server
    def constant(client):
        return constants.FOO

    assert constant(redis) == 3

def test_nested_func(redis):
    class constants:
        STRING = 'foo'

    @redis_server
    def replace(client):
        return constants.STRING.replace('foo', 'bar')

    assert replace(redis) == 'bar'

def test_string_replace(redis):
    @redis_server
    def replace(client, string):
        return string.replace('foo', 'bar')

    assert replace(redis, 'foo') == 'bar'

def test_string_join(redis):
    @redis_server
    def join(client, arr):
        return ', '.join(arr)

    assert join(redis, ['a', 'b', 'c']) == 'a, b, c'

def test_newline(redis):
    @redis_server
    def newline(client):
        return '\n'

    assert newline(redis) == '\n'

def test_usub(redis):
    @redis_server
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
    @redis_server
    def multiply(client, m, n):
        return m * n

    assert multiply(redis, 3, 4) == 12

def test_divide(redis):
    @redis_server
    def divide(client, m, n):
        return m / n

    assert divide(redis, 12, 4) == 3

def test_power(redis):
    @redis_server
    def power(client, m, n):
        return m ** n

    assert power(redis, 3, 3) == 27

def test_or(redis):
    @redis_server
    def bool_or(client):
        return True or False

    assert bool_or(redis)

def test_and(redis):
    @redis_server
    def bool_and(client):
        return True and False

    assert not bool_and(redis)
