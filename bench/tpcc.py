import sys
import time

sys.path.insert(0, '.')
from locomotor import redis_server

sys.path.insert(0, 'vendor/pytpcc')
sys.path.insert(0, 'vendor/pytpcc/pytpcc')
from pytpcc.drivers import redisdriver
from pytpcc.util import *
from pytpcc.runtime import *
from pytpcc import constants

NUM_WAREHOUSES = 4
SCALE_FACTOR = 50

class PartitionedDriver(redisdriver.RedisDriver):
    KEY_SEPARATOR = ':'

    def doDelivery(self, params):
        if self.debug['delivery'] != 'None':
            pass
        if self.debug['delivery'] == 'Verbose':
            pass

        # Setup Redis pipelining
        node = self.shard(params["w_id"])
        rdr = self.r_pipes[node]
        wtr = self.w_pipes[node]

        self._doDelivery(wtr, params)

    @redis_server
    def _doDelivery(self, client, params):
        # Initialize input parameters
        w_id = params["w_id"]
        o_carrier_id = params["o_carrier_id"]
        ol_delivery_d = params["ol_delivery_d"]

        # Initialize result set
        result = []

        #-------------------------
        # Initialize Data Holders
        #-------------------------
        order_key = []
        ol_total = []
        customer_key = []
        ol_counts = []
        no_o_id = []
        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1):
            order_key.append(None)
            ol_total.append(0)
            customer_key.append(None)
            ol_counts.append(0)

        #---------------------
        # Get New Order Query
        #---------------------
        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1):
            cursor = d_id - 1
            # Get set of possible new order ids
            index_key = self.safeKey([d_id, w_id])
            client.srandmember('NEW_ORDER.INDEXES.GETNEWORDER.' + index_key)
        id_set = client.execute()

        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1):
            cursor = d_id - 1
            if id_set[cursor] == None :
                client.get('NULL_VALUE')
            else:
                client.hget('NEW_ORDER.' + str(id_set[cursor]), 'NO_O_ID')
        no_o_id = client.execute()

        if self.debug['delivery'] == 'Verbose':
            pass

        #-----------------------
        # Get Customer ID Query
        #-----------------------
        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1):
            cursor = d_id - 1
            if no_o_id[cursor] == None :
                order_key.insert(cursor, 'NO_KEY')
            else:
                order_key.insert(
                        cursor,
                        self.safeKey([w_id, d_id, no_o_id[0]])
                        )
                client.hget('ORDERS.' + order_key[cursor], 'O_C_ID')
        c_id = client.execute()

        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1):
            cursor = d_id - 1
            if no_o_id[cursor] == None or c_id[cursor] == None:
                si_key = 'NO_KEY'
            else:
                si_key = self.safeKey([no_o_id[cursor], w_id, d_id])
            client.smembers('ORDER_LINE.INDEXES.SUMOLAMOUNT.' + si_key)
        ol_ids = client.execute()

        if self.debug['delivery'] == 'Verbose':
            pass

        #-----------------------------
        # Sum Order Line Amount Query
        #-----------------------------
        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1):
            cursor = d_id - 1
            if no_o_id[cursor] == None or c_id[cursor] == None:
                client.get('NULL_VALUE')
            else:
                for i in ol_ids[cursor]:
                    client.hget('ORDER_LINE.' + str(i), 'OL_AMOUNT')
                    ol_counts[cursor] += 1

        pipe_results = client.execute()
        index = 0
        counter = 0

        for ol_amount in pipe_results :
            counter += 1
            if counter > ol_counts[index]:
                index += 1
                counter = 0
            elif ol_amount != None :
                ol_total[index] += float(ol_amount)

        if self.debug['delivery'] == 'Verbose':
            pass

        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1):
            cursor = d_id - 1
            if no_o_id[cursor] == None or c_id[cursor] == None:
                ## No orders for this district: skip it.
                ## Note: This must be reported if > 1%
                continue

            #------------------------
            # Delete New Order Query
            #------------------------
            no_key = self.safeKey([d_id, w_id, no_o_id[cursor]])
            no_si_key = self.safeKey([d_id, w_id])
            client.delete('NEW_ORDER.' + no_key)
            client.srem('NEW_ORDER.IDS', no_key)
            client.srem('NEW_ORDER.INDEXES.GETNEWORDER.' + no_si_key, no_key)

            if self.debug['delivery'] == 'Verbose':
                pass

            #---------------------
            # Update Orders Query
            #---------------------
            client.hset(
                    'ORDERS.' + order_key[cursor],
                    'W_CARRIER_ID',
                    o_carrier_id
                    )

            if self.debug['delivery'] == 'Verbose':
                pass

            #-------------------------
            # Update Order Line Query
            #-------------------------
            for i in ol_ids[cursor]:
                client.hset(
                        'ORDER_LINE.' + str(i),
                        'OL_DELIVERY_D',
                        ol_delivery_d
                        )

                if self.debug['delivery'] == 'Verbose':
                    pass

        #-----------------------
        # Update Customer Query
        #-----------------------
        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1):
            cursor = d_id - 1
            if no_o_id[cursor] == None or c_id[cursor] == None :
                client.get('NULL_VALUE')
                customer_key.insert(cursor, 'NO_KEY')
            else:
                customer_key.insert(
                        cursor,
                        self.safeKey([w_id, d_id, c_id[cursor]])
                        )
                client.hget('CUSTOMER.' + customer_key[cursor], 'C_BALANCE')
        old_balance = client.execute()

        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1):
            cursor = d_id - 1
            if no_o_id[cursor] == None or c_id[cursor] == None :
                continue
            else:
                new_balance = float(old_balance[cursor]) + \
                              float(ol_total[cursor])
                client.hset(
                        'CUSTOMER.' + customer_key[cursor],
                        'C_BALANCE',
                        new_balance
                        )
                result.append((d_id, no_o_id[cursor]))
        client.execute()

        if self.debug['delivery'] == 'Verbose':
            pass
        if self.debug['delivery'] != 'None':
            pass

        return result

def bench(partition=False):
    global nurand

    # Construct a Redis driver object
    if partition:
        driver = PartitionedDriver(ddl=None)
    else:
        driver = redisdriver.RedisDriver(ddl=None)

    defaultConfig = driver.makeDefaultConfig()
    config = dict(map(lambda x: (x, defaultConfig[x][1]),
                      defaultConfig.keys()))
    config['reset'] = True
    driver.loadConfig(config)

    # Initialize the executor
    scaleParameters = scaleparameters.makeWithScaleFactor(NUM_WAREHOUSES,
                                                          SCALE_FACTOR)
    nurand = rand.setNURand(nurand.makeForLoad())
    e = executor.Executor(driver, scaleParameters, stop_on_error=True)

    # Load the data
    driver.loadStart()
    l = loader.Loader(driver, scaleParameters,
                      range(scaleParameters.starting_warehouse,
                            scaleParameters.ending_warehouse+1), True)
    l.execute()
    driver.loadFinish()

    # Run a bunch of doDelivery transactions
    driver.executeStart()
    start = time.time()
    for i in range (10000):
        params = e.generateDeliveryParams()
        driver.doDelivery(params)
    end = time.time()
    driver.executeFinish()

    print(end - start)

if __name__ == '__main__':
    bench(len(sys.argv) > 1 and sys.argv[1] == 'partition')
