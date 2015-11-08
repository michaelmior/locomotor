import ast
import imp
import pytest
import sully
import sys

from querybench import identify_redis

sys.path.insert(0, 'vendor/pytpcc')
sys.path.insert(0, 'vendor/pytpcc/pytpcc')
from pytpcc.drivers import redisdriver


def test_no_evidence():
    def no_redis():
        pass

    assert len(identify_redis(no_redis)) == 0

def test_insufficient_evidence():
    def maybe_redis():
        redis.get('foo')

    assert len(identify_redis(maybe_redis)) == 0

def test_mixed_evidence():
    def maybe_redis():
        redis.get('foo')
        redis.bar('foo')

    assert len(identify_redis(maybe_redis)) == 0

def test_good_evidence():
    def yes_redis():
        redis.get('foo')
        redis.set('foo', 'bar')

    assert len(identify_redis(yes_redis)) == 1

def test_tpcc_fragment():
    objs = identify_redis(redisdriver.RedisDriver.doDelivery)
    assert sully.nodes_equal(ast.Name(id='wtr', ctx=ast.Load()), objs[0])
    assert sully.nodes_equal(ast.Name(id='rdr', ctx=ast.Load()), objs[1])
