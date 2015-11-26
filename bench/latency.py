import redis
import time

def bench():
    client = redis.StrictRedis()
    start = time.time()

    for _ in range(100000):
        client.ping()
    end = time.time()

    print(end - start)

if __name__ == '__main__':
    bench()
