import imp
import pytest

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
