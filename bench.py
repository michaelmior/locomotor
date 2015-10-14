import random
import string
import time

import psycopg2
import pymongo
import redis

CATEGORIES = ['Books', 'Toys', 'Furniture', 'Clothing', 'Electronics',
              'Housewares', 'Tools', 'Groceries', 'Outdoors', 'Decor']

class Bench(object):
    def create(self):
        pass

    def drop(self):
        pass

    def insert(self, key, values):
        pass

    def get_by_id(self, key):
        pass

    def get_by_category(self, category):
        pass

class RedisBench(Bench):
    def __init__(self):
        self.client = redis.StrictRedis()

    def insert(self, key, values):
        self.client.hmset('item:' + str(key), values)
        self.client.lpush('category:' + values['category'], key)

    def drop(self):
        self.client.flushdb()

    def get_by_id(self, key):
        return self.client.hgetall('item:' + str(key))

    def get_by_category(self, category):
        ids = self.client.lrange('category:' + category, 0, -1)
        items = []
        for id in ids:
            items.append(self.client.hgetall('item:' + str(id)))

        return items

class MongoBench(Bench):
    def __init__(self):
        self.conn = pymongo.MongoClient()
        self.client = self.conn.querybench.items

    def create(self):
        self.client.create_index('category')

    def drop(self):
        self.conn.drop_database('querybench')

    def insert(self, key, values):
        values['_id'] = key
        self.client.insert_one(values)
    
    def get_by_id(self, key):
        return self.client.find_one({'_id': key})

    def get_by_category(self, category):
        return list(self.client.find({'category': category}))

class PostgresBench(Bench):
    def __init__(self):
        self.client = psycopg2.connect("user='querybench' " \
                                       "password='querybench' " \
                                       "host='127.0.0.1'")
        self.client.autocommit = True

    def create(self):
        try:
            cur = self.client.cursor()
            cur.execute("""
                 CREATE TABLE querybench(id integer PRIMARY KEY,
                                         name varchar(50),
                                         category varchar(50))""")
            cur.execute("CREATE INDEX ON querybench(category)")
        except:
            pass

    def drop(self):
        old_isolation_level = self.client.isolation_level
        self.client.set_isolation_level(0)
        self.client.cursor().execute("DROP TABLE querybench")
        self.client.set_isolation_level(old_isolation_level)

    def insert(self, key, values):
        self.client.cursor().execute("""
            INSERT INTO querybench(id, name, category)
            VALUES (%s, %s, %s)""",
            (key, values['name'], values['category']))

    def get_by_id(self, key):
        cur = self.client.cursor()
        cur.execute("SELECT * FROM querybench WHERE id=%s", (key,))
        return cur.fetchone()

    def get_by_category(self, category):
        cur = self.client.cursor()
        cur.execute("SELECT * FROM querybench WHERE category=%s", (category,))
        return cur.fetchall()

def main():
    random.shuffle(CATEGORIES)

    for cls in [RedisBench, MongoBench, PostgresBench]:
        bench = cls()
        bench.create()

        for i in range(1, 100000):
            name = ''.join(random.choice(string.ascii_uppercase)
                    for _ in range(50))
            bench.insert(i, {'name': name,
                             'category': CATEGORIES[i % len(CATEGORIES)]})

        start = time.time()
        for i in range(100):
            bench.get_by_id(random.randint(1, 100000))
        print cls.__name__, 'ID', time.time() - start

        start = time.time()
        for i in range(100):
            bench.get_by_category(CATEGORIES[i % len(CATEGORIES)])
        print cls.__name__, 'CAT', time.time() - start

        bench.drop()

if __name__ == '__main__':
    main()
