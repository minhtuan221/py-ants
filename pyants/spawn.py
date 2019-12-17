import multiprocessing
import queue
import threading
# Alternatively, you can create an instance of the loop manually, using: uvloop
import time
from collections import deque
from collections.abc import Iterable
from typing import List, Callable


class SyncThread(threading.Thread):
    """Threaded Sync reader, read data from queue"""

    def __init__(self, in_queue, out_queue=None, name=None, watching: deque = None):
        """Threaded Sync reader, read data from queue
        Arguments:
            queue {[type]} -- queue or deque
        Keyword Arguments:
            out_queue {[type]} -- queue receive result (default: {None})
        """

        threading.Thread.__init__(self, name=name)
        self.in_queue: queue.Queue = in_queue
        self.out_queue: queue.Queue = out_queue
        self.watching = watching

    def run(self):
        while True:
            # Grabs host from queue
            f, args, kwargs = self.in_queue.get()

            # Grabs item and put to input_function
            result = f(*args, **kwargs)

            if self.out_queue and result:
                if isinstance(result, Iterable):
                    self.out_queue.put(*result)
                else:
                    self.out_queue.put(*[result])

            if self.watching and len(self.watching) > 0:
                self.watching.popleft()
                # print('Watching list is:', self.watching)

            # Signals to queue job is done
            self.in_queue.task_done()
            # print('Thread complete task', self.getName())


class AsyncProcess(multiprocessing.Process):

    def __init__(self, in_queue, out_queue, name=None, watching: deque = None):
        multiprocessing.Process.__init__(self, name=name)
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.stop_event = multiprocessing.Event()
        self.watching = watching

    def stop(self):
        self.stop_event.set()

    def run(self):
        while not self.stop_event.is_set():
            # Grabs item from queue
            f, args, kwargs = self.in_queue.get()

            # Grabs item and put to input_function
            result = f(*args, **kwargs)

            if self.out_queue and result:
                if isinstance(result, Iterable):
                    self.out_queue.put(*result)
                else:
                    self.out_queue.put(*[result])

            if self.watching and len(self.watching) > 0:
                self.watching.popleft()

            # Signals to queue job is done
            self.in_queue.task_done()
            if self.stop_event.is_set():
                print('Process %s is stopping' % self.pid)
                break


class DistributedThreads(object):

    def __init__(self, out_queue=None, max_workers=4, max_watching=100, worker=None, maxsize=0, delay=0.005):
        self.out_queue = out_queue
        self.max_workers = max_workers
        self.max_watching = max_watching
        self.current_id = 0
        self.max_qsize = maxsize
        self.delay = delay
        self.queue_list = []
        self.watching_list = []
        self.worker_list = []
        if worker is None:
            self.init_worker()
        else:
            self.init_worker(worker=worker)

    def init_worker(self, worker=SyncThread):
        # create list of queue
        self.queue_list = [queue.Queue(maxsize=self.max_qsize) for _ in range(self.max_workers)]

        # create list of watching queue
        self.watching_list = [deque() for _ in range(self.max_workers)]

        # create list of threads:
        self.worker_list = []
        for i in range(self.max_workers):
            one_worker = worker(
                self.queue_list[i], out_queue=self.out_queue, name=str(i), watching=self.watching_list[i])
            one_worker.daemon = True
            self.worker_list.append(one_worker)
            one_worker.start()

    def iterate_queue(self, watching: list, key):
        if key is not None:
            watching.append(key)
        if len(watching) > self.max_watching:
            time.sleep(self.delay)

    def next_worker(self, last_id):
        return (last_id + 1) % self.max_workers

    def check_next_queue(self, current_queue_size, last_id):
        next_id = self.next_worker(last_id)
        if current_queue_size >= self.queue_list[next_id].qsize():
            return next_id
        else:
            return self.check_next_queue(current_queue_size, next_id)

    def choose_worker(self):
        current_queue_size = self.queue_list[self.current_id].qsize()
        return self.check_next_queue(current_queue_size, self.current_id)
        # return (self.current_id+1) % self.max_workers

    def submit(self, f: Callable, *args, **kwargs):
        return self.submit_id(None, f, *args, **kwargs)

    def submit_id(self, key, f, *args, **kwargs):
        worker_id = None
        # check if key belong to any worker
        if key is not None:
            for i in range(self.max_workers):
                if key in self.watching_list[i]:
                    if worker_id is not None:
                        raise ValueError("Key belong to more than one worker")
                    worker_id = i
                    self.current_id = worker_id
                    break
        # choosing a work_id if not
        if worker_id is None:
            worker_id = self.choose_worker()
            # print('choose queue =>', worker_id)
            self.current_id = worker_id
        # assign to worker and watching list
        worker = self.queue_list[worker_id]
        watching = self.watching_list[worker_id]

        # add key to a watching
        self.iterate_queue(watching, key)
        # print(worker_id, watching)
        # add function to queue
        worker.put((f, args, kwargs))

    def shutdown(self):
        for q in self.queue_list:
            q.join()


class DistributedProcess(DistributedThreads):
    def init_worker(self, worker=AsyncProcess):
        # create list of queue
        self.queue_list = [multiprocessing.JoinableQueue() for _ in range(self.max_workers)]

        # create list of watching queue
        self.watching_list = [deque() for i in range(self.max_workers)]

        # create list of threads:
        self.worker_list: List[AsyncProcess] = []
        for i in range(self.max_workers):
            one_worker = worker(
                self.queue_list[i], out_queue=self.out_queue, name=str(i))
            self.worker_list.append(one_worker)
            one_worker.start()

    def iterate_queue(self, watching: deque, key):
        if key is not None:
            watching.append(key)
        if len(watching) > self.max_watching:
            watching.popleft()

    def choose_worker(self):
        return (self.current_id + 1) % self.max_workers

    def shutdown(self):
        for q in self.queue_list:
            q.join()
        for process in self.worker_list:
            print('Distributed Process %s is stopping' % process.pid)
            process.terminate()


class Worker(threading.Thread):
    """Threaded title parser"""

    def __init__(self, out_queue, f):
        threading.Thread.__init__(self)
        self.out_queue = out_queue
        self.f = f

    def run(self):
        while True:
            # Grabs chunk from queue
            args = self.out_queue.get(), None

            # Parse the chunk
            args = args[:-1]
            self.f(*args)

            # Signals to queue job is done
            self.out_queue.task_done()

