# Locomotor

[![Build Status](https://travis-ci.org/michaelmior/locomotor.svg?branch=main)](https://travis-ci.org/michaelmior/locomotor)

Locomotor aims to automatically translate Python code which makes requests to a [Redis](https://redis.io/) server into equivalent [Lua](https://www.lua.org/) code which is executed using the [EVAL](https://redis.io/commands/eval) command.
This can result in significants speedups in the case where the code makes multiple requests since it avoids round trips between the server and the client.

Currently some minor work is required on the Python side.
First, you should isolate the code which you want to run as a Lua script in a single function where one of the parameters is a connection to the Redis server.
Then simply add the annotation `@locomotor.redis_server` to the function.

```python
from locomotor import redis_server

@redis_server(redis_objs=['redis'])
def get_many(redis, count):
    values = []
    for i in range(count):
        values.append(redis.get('KEY' + str(i)))

    return values
```

In this case, note that the parameter identifying the Redis server object was manually specified.
This is required if the heuristics used by Locomotor can't reliably determine how the server is accessed.
See [`bench/tpcc.py`](bench/tpcc.py) for an example of this determination being done automatically.

There are many limitations on the code which can be translated.
Most of these are because certain Python constructs haven't been implemented.
If you hit such a case, you'll see an `UntranslatableCodeException`.
Even if the code does appear translate correctly, you'll want to thoroughly test the translated version version.
