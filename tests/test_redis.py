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

        @redis_server
        def get_decorated(self, client, key):
            return client.get(self.decorate_key(key))

    redis.set('foofunc1', 'bar')
    assert Foo().get_decorated(redis, 'foofunc') == 'bar'

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
