# QueryBench

[![Build Status](https://travis-ci.org/michaelmior/querybench.svg)](https://travis-ci.org/michaelmior/querybench)

This is a series of experiments playing with the performance of queries executed using data models of different databases.
Currently there are implementations of a simple query using [Redis](http://redis.io/), [MongoDB](https://www.mongodb.com/), and [PostgreSQL](http://www.postgresql.org/).
The query involves a simple set of items which have an ID, a name, and a string representing the category of the item.
Running `bench.py` will execute a simple series of tests against each of these targets with `IN` representing insert performance, `ID` representing fetching items by ID and `CAT` representing fetching items by category.

A component of these tests involved writing a Lua script for Redis to perform some work server side.
This led to experimenting with automatic translation of Python code accessing Redis into Lua scripts.
The current implementation can be found in `querybench/__init__.py`.
In this file is a function decorator `@redis_server`.
When this is applied to a function where the first parameter is a [`StrictRedis`](https://redis-py.readthedocs.org/en/latest/index.html?highlight=strictredis#redis.StrictRedis) object, the decorator will attempt to rewrite the function in Lua so it can be executed on the server.
The things which are supported are *very* limited, but is enough to run some simple examples which you can see in `tests/test_redis.py`.
Tests can be run with [pytest](http://pytest.org/latest/) via `python setup.py test` and require a locally running [Redis](http://redis.io/) server.
Note that *all* data which is held in the server will be dropped after tests run.

There is also a Redis test involving a small change to Redis which can be found on [my fork](https://github.com/michaelmior/redis/tree/interface-test).
This adds the `hgetlist` command which accepts a key and a hash field.
The key is expected to contain a list consisting of keys which point to hashes.
The given field is retrieved from each hash and returned in a single list.
