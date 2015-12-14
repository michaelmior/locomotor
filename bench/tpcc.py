import argparse
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
ITERATIONS = 10000

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

        self._doDelivery(rdr, wtr, params)

    @redis_server
    def _doDelivery(self, rdr, wtr, params) :
        if self.debug['delivery'] != 'None' :
            print 'TXN DELIVERY STARTING ------------------'
            tt = time.time()
        if self.debug['delivery'] == 'Verbose' :
            t0 = tt

        # Initialize input parameters
        w_id = params["w_id"]
        o_carrier_id = params["o_carrier_id"]
        ol_delivery_d = params["ol_delivery_d"]

        # Initialize result set
        result = [ ]

        #-------------------------
        # Initialize Data Holders
        #-------------------------
        order_key = [ ]
        ol_total = [ ]
        customer_key = [ ]
        ol_counts = [ ]
        no_o_id = [ ]
        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1) :
            order_key.append(None)
            ol_total.append(0)
            customer_key.append(None)
            ol_counts.append(0)

        #---------------------
        # Get New Order Query
        #---------------------
        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1) :
            cursor = d_id - 1
            # Get set of possible new order ids
            index_key = self.safeKey([d_id, w_id])
            rdr.srandmember('NEW_ORDER.INDEXES.GETNEWORDER.' + index_key)
        id_set = rdr.execute()

        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1) :	
            cursor = d_id - 1
            if not id_set[cursor]:
                rdr.get('NULL_VALUE')
            else :
                rdr.hget('NEW_ORDER.' + str(id_set[cursor]), 'NO_O_ID')
        no_o_id = rdr.execute()

        if self.debug['delivery'] == 'Verbose' :
            print 'New Order Query: ', time.time() - t0
            t0 = time.time()

        #-----------------------
        # Get Customer ID Query
        #-----------------------
        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1) :	
            cursor = d_id - 1
            if not no_o_id[cursor]:
                order_key[cursor] = 'NO_KEY'
            else :
                order_key[cursor] = self.safeKey([w_id, d_id, no_o_id[0]])
            rdr.hget('ORDERS.' + order_key[cursor], 'O_C_ID')
        c_id = rdr.execute()

        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1) :	
            cursor = d_id - 1
            if not no_o_id[cursor] or not c_id[cursor]:
                si_key = 'NO_KEY'
            else :
                si_key = self.safeKey([no_o_id[cursor], d_id, w_id])
            rdr.smembers('ORDER_LINE.INDEXES.SUMOLAMOUNT.' + si_key)
        ol_ids = rdr.execute()

        if self.debug['delivery'] == 'Verbose' :
            print 'Get Customer ID Query:', time.time() - t0
            t0 = time.time()

        #-----------------------------
        # Sum Order Line Amount Query
        #-----------------------------
        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1) :	
            cursor = d_id - 1
            if not no_o_id[cursor] or not c_id[cursor]:
                rdr.get('NULL_VALUE')
            else :
                for i in ol_ids[cursor] :
                    rdr.hget('ORDER_LINE.' + str(i), 'OL_AMOUNT')
                    ol_counts[cursor] += 1

        pipe_results = rdr.execute()
        index = 0
        counter = 0

        for ol_amount in pipe_results : 
            counter += 1
            if counter > ol_counts[index] :
                index += 1
                counter = 0
            elif ol_amount:
                ol_total[index] += float(ol_amount)

        if self.debug['delivery'] == 'Verbose' :
            print 'Sum Order Line Query:', time.time() - t0
            t0 = time.time()

        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1) :	
            cursor = d_id - 1
            if not no_o_id[cursor] or not c_id[cursor]:
                ## No orders for this district: skip it. 
                ## Note: This must be reported if > 1%
                continue

            #------------------------
            # Delete New Order Query
            #------------------------
            no_key = self.safeKey([d_id, w_id, no_o_id[cursor]])
            no_si_key = self.safeKey([d_id, w_id])
            wtr.delete('NEW_ORDER.' + no_key)
            wtr.srem('NEW_ORDER.IDS', no_key)
            wtr.srem('NEW_ORDER.INDEXES.GETNEWORDER.' + no_si_key, no_key)

            if self.debug['delivery'] == 'Verbose' :
                print 'Delete New Order Query:', time.time() - t0
                t0 = time.time()

            #---------------------
            # Update Orders Query
            #---------------------
            wtr.hset(
                    'ORDERS.' + order_key[cursor], 
                    'W_CARRIER_ID', 
                    o_carrier_id
                    )

            if self.debug['delivery'] == 'Verbose' :
                print 'Update Orders Query:', time.time() - t0
                t0 = time.time()

            #-------------------------
            # Update Order Line Query
            #-------------------------
            for i in ol_ids[cursor] :
                wtr.hset(
                        'ORDER_LINE.' + str(i),
                        'OL_DELIVERY_D', 
                        ol_delivery_d
                        )

                if self.debug['delivery'] == 'Verbose' :
                    print 'Update Order Line Query:', time.time() - t0
                t0 = time.time()
        wtr.execute()

        #-----------------------
        # Update Customer Query
        #-----------------------
        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1) :	
            cursor = d_id - 1
            if not no_o_id[cursor] or not c_id[cursor]:
                rdr.get('NULL_VALUE')
                customer_key[cursor] = 'NO_KEY'
            else :
                customer_key[cursor] = self.safeKey([w_id, d_id, c_id[cursor]])
                rdr.hget('CUSTOMER.' + customer_key[cursor], 'C_BALANCE')
        old_balance = rdr.execute()

        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1) :
            cursor = d_id - 1
            if not no_o_id[cursor] or not c_id[cursor]:
                continue
            else :
                new_balance = float(old_balance[cursor]) + float(ol_total[cursor])
                wtr.hset(
                        'CUSTOMER.' + customer_key[cursor],
                        'C_BALANCE', 
                        new_balance
                        )
                result.append((d_id, no_o_id[cursor]))
        wtr.execute()

        if self.debug['delivery'] == 'Verbose' :
            print 'Update Customer Query:', time.time() - t0
        if self.debug['delivery'] != 'None' :
            print 'TXN DELIVERY:', time.time() - tt

        return result
    # End doDelivery()

def bench(partition=False, execute=True):
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

    # Ensure we have orders to process
    print(scaleParameters)

    nurand = rand.setNURand(nurand.makeForLoad())
    e = executor.Executor(driver, scaleParameters, stop_on_error=True)

    # Load the data
    driver.loadStart()
    l = loader.Loader(driver, scaleParameters,
                      range(scaleParameters.starting_warehouse,
                            scaleParameters.ending_warehouse+1), True)
    l.execute()
    driver.loadFinish()

    if execute:
        # Run a bunch of doDelivery transactions
        driver.executeStart()
        start = time.time()
        for i in range (ITERATIONS):
            params = e.generateDeliveryParams()
            driver.doDelivery(params)
        end = time.time()
        driver.executeFinish()

        print(end - start)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Python/Redis TPC-C benchmark')

    parser.add_argument('--no-execute', dest='execute', action='store_false',
                        default=True,
                        help='skip executing and only load the data')
    parser.add_argument('partition', nargs='?', default='')

    args = parser.parse_args()
    bench(args.partition == 'partition', execute=args.execute)
