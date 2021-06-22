import multiprocessing
import queue
import threading
from concurrent.futures import ThreadPoolExecutor
# Alternatively, you can create an instance of the loop manually, using: uvloop
import time
import signal
import random
from collections import deque
from collections.abc import Iterable
import traceback
from typing import List, Callable

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


class Task:
    
    def __init__(self, task_id, f, args, kwargs):
        self.id = task_id
        self.f = f
        self.args = args
        self.kwargs = kwargs
        self.result = None
        self.error = None
        self.is_done = False
        self.key = None # must set manually
    
    def wrap(self, t):
        self.id = t.id
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
        # print(e)

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
        print('process was created')

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
    
    def __init__(self, in_queue=None, out_queue=None, max_workers=None, max_task=1000, worker=None, maxsize=0, delay=0.001):
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
            max_workers = multiprocessing.cpu_count()*2    
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
    
    def choose_worker(self)->TaskProcess:
        return random.choice(self.worker_list)
    
    def gen_task_id(self):
        self.current_id += 1
        return self.current_id

    def __submit(self, f, args, kwargs):
        t = Task(self.gen_task_id(), f, args, kwargs)
        self.in_queue.put(t)
        self.waiting_task.add(t.id)
        self.map_task[t.id] = t
        return t
    
    def submit(self, f: Callable, *args, **kwargs)-> Task:
        try:
            if self.is_alive == False:
                raise Exception('TaskPool is already closed')
            return self.__submit(f, args, kwargs)
        except Exception as e:
            print('start closing all child process')
            traceback.print_exc()
            self.force_shutdown()
    
    def consume_result(self):
        while True:
            if len(self.waiting_task)==0:
                # print('waiting_task is empty')
                break
            time.sleep(self.delay)
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
                print('Distributed Process %s is stopping' % process.pid)
                process.terminate()
        except:
            traceback.print_exc()
            self.force_shutdown()
            return

def say_after(delay, what):
    time.sleep(0.1)
    print(what)

def test():
    pool = TaskPool(max_workers=2)
    for i in range(1000):
        try:
            t = pool.submit(say_after, random.randint(1,5)/5, 'task '+str(i))
        except:
            traceback.print_exc()
            break
    pool.shutdown()

if __name__ == "__main__":
    test()
