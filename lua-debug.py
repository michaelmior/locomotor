import locomotor
import redis

r = redis.StrictRedis()
p = r.pubsub()
p.subscribe(locomotor.DEBUG_LOG_CHANNEL, ignore_subscribe_messages=True)

try:
    print('Listening for debug messages...')
    for msg in p.listen():
        if msg['type'] in ('message', 'pmessage'):
            print(msg['data'])
except KeyboardInterrupt:
    pass
