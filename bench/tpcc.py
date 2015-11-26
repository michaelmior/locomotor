import sys
import time

sys.path.insert(0, 'vendor/pytpcc')
sys.path.insert(0, 'vendor/pytpcc/pytpcc')
from pytpcc.drivers import redisdriver
from pytpcc.util import *
from pytpcc.runtime import *

NUM_WAREHOUSES = 4
SCALE_FACTOR = 50

def bench():
    global nurand

    # Construct a Redis driver object
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
    bench()
