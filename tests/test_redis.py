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
    @redis_server
    def increx(client, key):
        if client.exists(key) == 1:
            return client.incr(key)

    assert increx(redis, 'fooincr') == None
    redis.set('fooincr', 1)
    assert increx(redis, 'fooincr') == 2
