from ast import Try
import multiprocessing
import queue
import signal
import threading
from concurrent.futures import ThreadPoolExecutor
# Alternatively, you can create an instance of the loop manually, using: uvloop
import time
import os
import random
from collections import deque
from collections.abc import Iterable
from tkinter import W
import traceback
from typing import List, Callable, Union, Optional


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()

        print('%r  %2.4f ms' %
              (method.__name__, (te - ts) * 1000))
        return result

    return timed


# https://stackoverflow.com/questions/64917285/difference-in-python-thread-join-between-python-3-7-and-3-8


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
            # Grabs task from queue
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

    def __init__(self, in_queue, out_queue, stop_event: multiprocessing.Event, name=None, watching: deque = None):
        multiprocessing.Process.__init__(self, name=name)
        self.in_queue: multiprocessing.Queue = in_queue
        self.out_queue = out_queue
        self.stop_event = stop_event
        self.watching = watching
    
    def stop(self, *args):
        self.stop_event.set()

    def run(self):
        # signal.signal(signal.SIGINT, signal.SIG_IGN)
        while True:
            # Grabs item from queue
            f, args, kwargs = self.in_queue.get()

            # Grabs item and put to input_function
            try:
                result = f(*args, **kwargs)
            except Exception as e:
                result = e

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
                break


class DistributedThreads(object):

    def __init__(self, out_queue=None, max_workers=4, max_watching=2**16, worker=None, maxsize=100, delay=0.005):
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

    def submit_id(self, key: int, f: Callable, *args, **kwargs):
        worker_id = None
        # check if key belong to any worker
        if key is not None:
            for i in range(self.max_workers):
                if key in self.watching_list[i]:
                    worker_id = i
                    self.current_id = worker_id
                    break

        if worker_id is None:
            # choosing a work_id if key is None
            worker_id = self.choose_worker()
            self.current_id = worker_id

        # assign to worker and watching list
        worker = self.queue_list[worker_id]
        watching = self.watching_list[worker_id]

        # add key to a watching
        self.iterate_queue(watching, key)
        # print(key, 'choose queue =>', worker_id)
        # add function to queue
        while True:
            try:
                worker.put((f, args, kwargs))
                break
            except queue.Empty:
                time.sleep(0.01)
                continue

    def shutdown(self):
        for q in self.queue_list:
            q.join()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.shutdown()


class RebalanceDistributedProcess(DistributedThreads):

    def init_worker(self, worker=AsyncProcess):
        # create list of queue
        self.queue_list = [multiprocessing.JoinableQueue(self.max_qsize) for _ in range(self.max_workers)]

        # create list of watching queue
        self.watching_list = [deque() for _ in range(self.max_workers)]

        # create list of threads:
        self.worker_list: List[AsyncProcess] = []
        stop_event=multiprocessing.Event()
        for i in range(self.max_workers):
            one_worker = AsyncProcess(
                self.queue_list[i], out_queue=self.out_queue, stop_event=stop_event, name=str(i))
            self.worker_list.append(one_worker)
            one_worker.daemon = True
            one_worker.start()

    def iterate_queue(self, watching: deque, key):
        if key is not None:
            watching.append(key)
        if len(watching) > self.max_watching:
            watching.popleft()

    def choose_worker(self):
        return (self.current_id + 1) % self.max_workers

    def shutdown(self):
        try:
            for q in self.queue_list:
                q.join()
        except Exception as e:
            traceback.print_exc()
            for process in self.worker_list:
                print('Distributed Process %s is forced to stop' % process.pid)
                process.terminate()
                process.join()


class DistributedProcess(object):

    def __init__(self, out_queue=None, max_workers=4, max_watching=2**16, worker=None, maxsize=100, delay=0.005):
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

    def init_worker(self, worker=AsyncProcess):
        # create list of queue
        self.queue_list = [multiprocessing.JoinableQueue(self.max_qsize) for _ in range(self.max_workers)]

        # create list of watching queue
        self.watching_list = [deque() for _ in range(self.max_workers)]

        # create list of threads:
        self.worker_list: List[AsyncProcess] = []
        stop_event=multiprocessing.Event()
        for i in range(self.max_workers):
            one_worker = AsyncProcess(
                self.queue_list[i], out_queue=self.out_queue, stop_event=stop_event, name=str(i))
            self.worker_list.append(one_worker)
            one_worker.daemon = True
            one_worker.start()
        
    def hash_to_choose_worker(self, key) -> int:
        if key is None:
            k = random.randint(0, self.max_workers-1)
        else:
            try:
                k = hash(key) % self.max_workers
            except TypeError:
                k = random.randint(0, self.max_workers-1)
        return k
    
    def submit(self, f: Callable, *args, **kwargs):
        return self.submit_id(None, f, *args, **kwargs)

    def submit_id(self, key: Union[int, str, float, None], f: Callable, *args, **kwargs):
        # hash the key
        worker_id = self.hash_to_choose_worker(key)

        # assign to worker and watching list
        worker = self.queue_list[worker_id]
        # no need watching list
        # print(key, 'choose queue =>', worker_id)

        # add function to queue
        while True:
            # this will block until it can put function into queue
            try:
                worker.put((f, args, kwargs))
                break
            except queue.Full:
                time.sleep(0.01)
                continue

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.shutdown()

    def shutdown(self):
        try:
            for q in self.queue_list:
                q.join()
        except Exception as e:
            traceback.print_exc()
            for process in self.worker_list:
                print('Distributed Process %s is forced to stop' % process.pid)
                process.terminate()
                process.join()


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


class Task:

    def __init__(self, task_id, f, args, kwargs):
        self.id = task_id
        self.f = f
        self.args = args
        self.kwargs = kwargs
        self.result = None
        self.error = None
        self.is_done = False
        self.key = None  # must set manually

    def wrap(self, t):
        self.f = t.f
        self.args = t.args
        self.kwargs = t.kwargs
        self.result = t.result
        self.error = t.error


def run_task(t: Task, out_queue):
    # Grabs item and put to input_function
    try:
        t.result = t.f(*t.args, **t.kwargs)
    except Exception as e:
        t.error = e
        print(e)

    t.is_done = True

    if out_queue:
        out_queue.put(t)


class TaskProcess(multiprocessing.Process):

    def __init__(self, in_queue, out_queue=None, name=None, refresh_delay=0.001):
        multiprocessing.Process.__init__(self, name=name)
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.stop_event = multiprocessing.Event()
        self.refresh_delay = refresh_delay

    def stop(self):
        self.stop_event.set()

    def run(self):
        with ThreadPoolExecutor() as executor:
            while True:
                # Grabs item from queue
                t: Task = self.in_queue.get()

                executor.submit(run_task, t, self.out_queue)

                # Signals to queue job is done
                self.in_queue.task_done()
                if self.stop_event.is_set():
                    print('Process %s is stopping' % self.pid)
                    break
                time.sleep(self.refresh_delay)
        print(self.name, 'process is closed')


class TaskPool(object):

    def __init__(self, in_queue=None, out_queue=None, max_workers=None, max_task=1000, worker=None, maxsize=0,
                 delay=0.001):
        """Task Pool: receive function and args and return task_id => convert to task => put task in the queue to
        a map of process => receive task done in other side => put task done in a queue
        Choose process: process can be choosed randomly or balance
        out queue: will be create automaticlly or passed in input, is multiprocessing.Queue() by default
        task id: any task is manage by task id, task id is an integer number that is unique
        task error: error when execute task will be save in task object and return back to taskpool
        process maintainer: a number of process will be maintain by task pool
        Args:
            max_workers (int, optional): [description]. Defaults to 4.
            max_task (int, optional): [description]. Defaults to 100.
            worker ([type], optional): [description]. Defaults to None.
            maxsize (int, optional): [description]. Defaults to 0.
            delay (float, optional): [description]. Defaults to 0.005.
        """
        if not in_queue:
            in_queue = multiprocessing.JoinableQueue()
        if not out_queue:
            out_queue = multiprocessing.Queue()
        if not max_workers:
            max_workers = multiprocessing.cpu_count() * 2
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.max_workers = max_workers
        self.max_task = max_task
        self.current_id = 0
        self.max_qsize = maxsize
        self.delay = delay
        self.worker_list = []
        self.map_task = dict()
        self.waiting_task = set()
        self.is_alive = True
        if worker is None:
            self.init_worker()
        else:
            self.init_worker(worker=worker)

    def init_worker(self, worker=TaskProcess):
        # create list of threads:
        self.worker_list: List[TaskProcess] = []
        for i in range(self.max_workers):
            one_worker = worker(self.in_queue, out_queue=self.out_queue, name=str(i), refresh_delay=self.delay)
            self.worker_list.append(one_worker)
            one_worker.daemon = True
            one_worker.start()

    def choose_worker(self) -> TaskProcess:
        return random.choice(self.worker_list)

    def gen_task_id(self):
        self.current_id += 1
        return self.current_id

    def __submit(self, f, args, kwargs, id=None):
        if id is None:
            t = Task(self.gen_task_id(), f, args, kwargs)
        else:
            t = Task(id, f, args, kwargs)
        self.in_queue.put(t)
        self.waiting_task.add(t.id)
        self.map_task[t.id] = t
        return t

    def add_task(self, t: Task):
        self.in_queue.put(t)
        self.waiting_task.add(t.id)
        self.map_task[t.id] = t
        return t

    def submit_id(self, _id: str, f: Callable, *args, **kwargs) -> Task:
        try:
            if not self.is_alive:
                raise Exception('TaskPool is already closed')
            return self.__submit(f, args, kwargs, id=_id)
        except Exception as e:
            print('start closing all child process')
            traceback.print_exc()
            self.force_shutdown()

    def submit(self, f: Callable, *args, **kwargs) -> Task:
        return self.submit_id(None, f, *args, **kwargs)

    def consume_result(self):
        while True:
            if len(self.waiting_task) == 0:
                # print('waiting_task is empty')
                break
            time.sleep(self.delay)
            q_size = self.out_queue.qsize()
            for i in range(q_size):
                t_done: Task = self.out_queue.get()
                if t_done.id in self.waiting_task:
                    self.map_task[t_done.id].wrap(t_done)
                    self.waiting_task.remove(t_done.id)
                else:
                    raise ValueError('task id of result not in map of task')
        return

    def get_result(self, task_id):
        if task_id in self.map_task:
            t: Task = self.map_task[task_id]
            if t.error:
                raise t.error
            return t.result
        raise ValueError('task id not in map of task')

    def force_shutdown(self):
        self.is_alive = False
        self.in_queue.cancel_join_thread()
        # self.in_queue.close()
        self.out_queue.close()
        for process in self.worker_list:
            print('Force Process %s is stopping' % process.pid)
            process.terminate()

    def shutdown(self):
        try:
            self.in_queue.join()
            self.consume_result()
            self.is_alive = False
            for process in self.worker_list:
                print('Task Process %s is stopping' % process.pid)
                process.terminate()
        except:
            traceback.print_exc()
            self.force_shutdown()
            return


def say_after(delay, what):
    time.sleep(delay)
    print(os.getpid(), what)
    # raise ValueError('test raise error')


@timeit
def test_one_process():
    for i in range(1000):
        say_after(random.randint(1, 20) / 1000, 'task ' + str(i))


@timeit
def test_task_pool(w=2):
    pool = TaskPool(max_workers=w)
    for i in range(1000):
        pool.submit_id(None, say_after, random.randint(1, 20) / 1000, 'task ' + str(i))
    pool.shutdown()
    print('worker =', w)


@timeit
def test_distributed_process_with_shutdown(w=2):
    pool = DistributedProcess(max_workers=w)
    for i in range(1000):
        pool.submit_id(i % 2**8, say_after, random.randint(1, 20) / 1000, 'task ' + str(i))
    pool.shutdown()
    print('worker =', w)


@timeit
def test_rebalance_distributed_process_with_shutdown(w=2):
    pool = RebalanceDistributedProcess(max_workers=w)
    for i in range(1000):
        pool.submit_id(i % 2**8, say_after, random.randint(1, 20) / 1000, 'task ' + str(i))
    pool.shutdown()
    print('worker =', w)


@timeit
def test_distributed_process_without_shutdown(w=2):
    pool = DistributedProcess(max_workers=w)
    for i in range(1000):
        pool.submit_id(i % 2**8, say_after, 0.01, 'task ' + str(i))
    print('worker =', w)
    # time.sleep(1)
    print('timeout')


@timeit
def test_distributed_process_context():
    with DistributedProcess(max_workers=2) as pool:
        for i in range(200):
            pool.submit_id(i % 2**1, say_after, random.randint(1, 20) / 1000, 'task ' + str(i))


if __name__ == "__main__":
    # các case push vào dưới 100 phần tử/joinableQueue thì ok, lớn hơn sẽ gây ra lỗi không thoát dc process

    # ----------------------- hashing key pool worker --------------------------- 
    # test with random time to complete (random sleep time)
    test_distributed_process_with_shutdown(1) # 'test_distributed_process_with_shutdown'  20362.6773 ms
    test_distributed_process_with_shutdown(2) # 'test_distributed_process_with_shutdown'  10235.3477 ms
    test_distributed_process_with_shutdown(4) # 'test_distributed_process_with_shutdown'  5442.7423 ms
    test_distributed_process_with_shutdown(8) # 'test_distributed_process_with_shutdown'  2921.3138 ms
    test_distributed_process_with_shutdown(16) # 'test_distributed_process_with_shutdown'  1958.2040 ms

    # ----------------------- rebalance key pool worker --------------------------- 
    # test with random time to complete (random sleep time)
    test_rebalance_distributed_process_with_shutdown(1) # 'test_rebalance_distributed_process_with_shutdown'  19833.1923 ms
    test_rebalance_distributed_process_with_shutdown(2) # 'test_rebalance_distributed_process_with_shutdown'  10114.3961 ms
    test_rebalance_distributed_process_with_shutdown(4) # 'test_rebalance_distributed_process_with_shutdown'  5311.5776 ms
    test_rebalance_distributed_process_with_shutdown(8) # 'test_rebalance_distributed_process_with_shutdown'  3022.2836 ms
    test_rebalance_distributed_process_with_shutdown(16) # 'test_rebalance_distributed_process_with_shutdown'  1926.1019 ms

    # ----------------------- test task pool---------------------------
    # test with random time to complete (random sleep time)
    test_one_process()  # 'test_one_process'  19961.0634 ms
    test_task_pool(1) # 1 worker : 'test_task_pool'  16203.1040 ms
    test_task_pool(2) # 2 worker : 'test_task_pool'  8178.5283 ms
    test_task_pool(4) # 4 worker : 'test_task_pool'  4228.0197 ms
    test_task_pool(8) # 8 worker : 'test_task_pool'  2581.5420 ms
    test_task_pool(16) # 16 worker : 'test_task_pool'  1604.7373 ms
