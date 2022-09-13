# https://superfastpython.com/multiprocessing-pool-map/
from time import sleep
from random import random
import multiprocessing as mp
from multiprocessing import Pool

# task executed in a worker process
def task(identifier):
    # generate a value
    value = random()
    # report a message
    print(f'Task {identifier} executing with {value}', flush=True)
    # block for a moment
    sleep(value)
    # return the generated value
    return value

# protect the entry point
if __name__ == '__main__':
    def example_pool_for_loop():
        # create and configure the process pool
        with Pool() as pool:
            # execute tasks in order
            for result in pool.map(task, range(10)):
                print(f'Got result: {result}', flush=True)
        # process pool is closed automatically
    # example_pool_for_loop()

    def example_pool_map():
        # create and configure the process pool
        with Pool() as pool:
            # execute tasks, block until all completed
            pool.map(task, range(10))
        # process pool is closed automatically
    # example_pool_map()

    def example_pool_map_without_chunksize():
        # create and configure the process pool
        with Pool(4) as pool:
            # execute tasks, block until all complete
            pool.map(task, range(40))
        # process pool is closed automatically
    # example_pool_map_without_chunksize()

    def example_pool_map_with_chunksize():
        # create and configure the process pool
        with Pool(4) as pool:
            # execute tasks, block until all complete
            pool.map(task, range(40), chunksize = 10)
        # process pool is closed automatically
    # example_pool_map_with_chunksize()