import ast
import imp
import pytest
import sully

from querybench import identify_redis


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
    constants = imp.load_source('constants',
                                'vendor/pytpcc/pytpcc/constants.py')
    abstractdriver = imp.load_source('abstractdriver',
                                     'vendor/pytpcc/pytpcc/drivers/' \
                                             'abstractdriver.py')
    redisdriver = imp.load_source('redisdriver',
                                  'vendor/pytpcc/pytpcc/drivers/' \
                                          'redisdriver.py')

    objs = identify_redis(redisdriver.RedisDriver.doDelivery)
    assert sully.nodes_equal(ast.Name(id='wtr', ctx=ast.Load()), objs[0])
    assert sully.nodes_equal(ast.Name(id='rdr', ctx=ast.Load()), objs[1])
