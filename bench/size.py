import redis
import time

def bench():
    client = redis.StrictRedis()

    for size in [10, 100, 1000, 10000, 100000, 1000000]:
        client.set('locobench', '1' * size)

        start = time.time()
        for _ in range(100000):
            client.get('locobench')
        end = time.time()
        print('%d,%f' % (size, end - start))

    client.delete('locobench')

if __name__ == '__main__':
    bench()
