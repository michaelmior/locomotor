import locomotor
import redis

r = redis.StrictRedis()
p = r.pubsub()
p.subscribe(locomotor.DEBUG_LOG_CHANNEL, ignore_subscribe_messages=True)

for msg in p.listen():
    print(msg['data'])
