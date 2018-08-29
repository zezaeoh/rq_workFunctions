# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os
import time

from rq import Connection, Queue
from redis import Redis

from workFunctions import dynamo_pipe_line


def main():
    # Range of Fibonacci numbers to compute
    fib_range = range(20, 34)
    # Kick off the tasks asynchronously
    async_results = {}
    host = None # add Redis server ip
    port = None # add Redis server port
    q = Queue(connection=Redis(host=host, port=port))
    for x in fib_range:
        d = {'test_column': x}
        async_results[x] = q.enqueue(dynamo_pipe_line, d, 'content_test')

    start_time = time.time()
    done = False
    while not done:
        os.system('clear')
        print('Asynchronously: (now = %.2f)' % (time.time() - start_time,))
        done = True
        for x in fib_range:
            result = async_results[x].return_value
            if result is None:
                done = False
                result = '(calculating)'
            print('test', x, ':', result)
            # print('fib(%d) = %s' % (x, result))
        print('')
        print('To start the actual in the background, run a worker:')
        print('    python examples/run_worker.py')
        time.sleep(0.2)

    print('Done')


if __name__ == '__main__':
    # Tell RQ what Redis connection to use
    f = Connection()
    with Connection():
        main()
