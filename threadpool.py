# Copyright (c) 2016, Sean Lydon
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * The names of its contributors may be used to endorse or promote products
#       derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL SEAN LYDON BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

from threading import Thread
try:
    from Queue import Queue
except ImportError:
    from queue import Queue

class ThreadPool:
    """This is a threadpool for finite I/O heavy workloads.  Like a unordered parallel collection.
    
    Typically a user will initialize a threadpool to handle many parallel I/O operations.  For example
    checking the existence of a bunch of URLs.
    1) Create a threadpool with a function to check a single url.
    2) Send all the URLs into the threadpool as tasks.
    3) Collect the results.  
    
    If the function throws an Exception, the task will be retried up to the number of retries specified
    when initializing the threadpool.  If the function still fails after X number of retries, then `None`
    is returned.
    
    `add_task` will start blocking as work backs up, and `wait_completion` will wait for all work to
    finish.  The results come back as a dictionary keyed by a collection of arguments and the return
    value of the function.
    
    ```
    >>> import requests
    >>> from threadpool import ThreadPool
    >>> def check_url(url):
    ...   return requests.head(url).status_code
    ...
    >>> tp = ThreadPool(check_url)
    >>> for url in collection_of_urls:
    ...   tp.add_task(url)
    ...
    >>> results = tp.wait_completion()
    >>> results.items()[0]
    (('http://www.google.com',), 200)
    ```
    
    Note:
    This mechanism holds all results on a Queue which could run a process out of memory if there
    are many (or large) results.  To properly handle this case, please use the ThreadPoolProducer
    and ThreadPoolConsumer classes.
    
    Note:
    An instance of this class doesn't clean up its threads, so make sure you `change_function`
    when you want to change the capabilities of this threadpool.  If you end up having a ton of
    unreferenced daemon threads attached to your process.
    
    Reference:
    http://code.activestate.com/recipes/577187-python-thread-pool/
    """
    class Worker(Thread):
        def __init__(self, tasks, results):
            Thread.__init__(self)
            self.tasks = tasks
            self.results = results
            self.daemon = True
            self.start()

        def do_fn_with_retries(self, func, args, kwargs, retries):
            res = None
            try:
                res = func(*args, **kwargs)
            except KeyboardInterrupt:
                raise
            except Exception:
                if retries > 0:
                    return self.do_fn_with_retries(func, args, kwargs, retries-1)
            return res

        def run(self):
            while True:
                func, args, kwargs, retries = self.tasks.get()
                res = self.do_fn_with_retries(func, args, kwargs, retries)
                self.results.put((args,res))
                self.tasks.task_done()

    def __init__(self, func=lambda x: x, num_threads=100, retries=1):
        """Initialize a threadpool.
        
        Keyword arguments:
        func -- the function to apply to tasks (default identity)
        num_threads -- the number of threads to use (default 100)
        retries -- the number of retries to attempt (default 1)
        """
        self.func = func
        self.retries = retries
        self.tasks = Queue(num_threads)
        self.results = Queue()
        self.threads = [self.Worker(self.tasks, self.results) for _ in range(num_threads)]

    def add_task(self, *args, **kargs):
        self.tasks.put((self.func, args, kargs, self.retries))

    def change_function(self, func):
        self.func = func

    def wait_completion(self):
        self.tasks.join()
        results = {}
        while not self.results.empty():
            args, res = self.results.get()
            results[args] = res
        return results

class ThreadPoolProducer(Thread):
    """This class is used for when a user needs to feed a threadpool asynchronously.
    
    An example of this is infinitely consuming from kafka.
    ```
    >>> from kafka import KafkaConsumer
    >>> from threadpool import *
    >>> tp = ThreadPool()
    >>> def consume_forever():
    ...   kafka_consumer = KafkaConsumer('my_favorite_topic')
    ...   for msg in kafka_consumer:
    ...     yield msg
    ...
    >>> tp_producer = ThreadPoolProducer(tp, consume_forever)
    >>> tp_producer.start()
    ```
    
    Note:
    Usually you want to start your consumer before your producer.
    """
    def __init__(self, tp, paramgen):
        Thread.__init__(self)
        self.tp = tp
        self.paramgen = paramgen

    def run(self):
        for args in self.paramgen:
            self.tp.add_task(*args)

class ThreadPoolConsumer(Thread):
    """This class is used for when a user needs to asynchronously consume from a threadpool.
    
    An example of this is writing 100k object results to a file.
    ```
    >>> import json
    >>> from threadpool import *
    >>> tp = ThreadPool()
    >>> fp = open('/tmp/results.txt', 'w')
    >>> def persist(args, res):
    ...   if res is not None:
    ...     fp.write(json.dumps(res))
    ...     fp.write('\n')
    >>> tp_consumer = ThreadPoolProducer(tp, persist, 100000)
    >>> tp_consumer.start()
    ```
    
    Note:
    Usually you want to start your consumer before your producer.
    """
    def __init__(self, tp, func, size):
        Thread.__init__(self)
        self.tp = tp
        self.func = func
        self.size = size

    def run(self):
        processed = 0
        while processed < self.size:
            args, res = self.tp.results.get()
            processed += 1
            self.func(args, res)

