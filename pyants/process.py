import multiprocessing as mp
import queue
import threading
# Alternatively, you can create an instance of the loop manually, using: uvloop
import time
from collections import deque
from collections.abc import Iterable
from typing import List, Callable, Dict, Awaitable, Sequence, Any
import asyncio
import random
import os
import traceback


class AsyncProcess(mp.Process):

    def __init__(self, in_queue, out_queue=None, name=None, max_task=100, sleep=0.005, max_queue=175):
        """Each process will have an async event loop. Event loop will consume multiprocessing queue in a while loop then put result in another 
        process queue
        """
        mp.Process.__init__(self, name=name)
        self.in_queue: mp.Queue = in_queue
        self.out_queue: mp.Queue = out_queue
        self.task_list: deque = deque()
        self.stop_event = mp.Event()
        self.max_task = max_task
        self.sleep = sleep
        self.max_queue = max_queue

    def stop(self):
        self.stop_event.set()

    async def process(self, task_id, f, *args, **kwargs):
        res = None
        tb = None
        try:
            res = await f(*args, **kwargs)
        except Exception as e:
            traceback.print_exc()
            tb = traceback.format_exc()
            raise e

        if self.out_queue:
            self.out_queue.put_nowait((task_id, res, tb))

    def produce(self, task_id, f, *args, **kwargs):
        while True:
            if self.in_queue.qsize() <= self.max_queue and not self.in_queue.full():
                self.in_queue.put_nowait((task_id, f, args, kwargs))
                break
            time.sleep(0.005)

    async def gather_task(self, total=None):
        bound = total or min(self.max_task, len(self.task_list))
        task_list = []
        for _ in range(bound):
            task_id, f, args, kwargs = self.task_list.popleft()
            task_list.append(self.process(task_id, f, *args, **kwargs))
        if len(task_list)>0:
            await asyncio.gather(*task_list)

    async def consume(self):
        while not self.stop_event.is_set():
            bound = self.in_queue.qsize()
            if len(self.task_list)>400:
                bound = 0
            # process job in internal queue
            # wait for an item from the producer
            if bound > 0:
                for _ in range(bound):
                    if self.in_queue.empty():
                        # print('bound = ', bound, self.in_queue.qsize())
                        time.sleep(self.sleep)
                        continue
                    # Grabs item from queue
                    task_id, f, args, kwargs = self.in_queue.get_nowait()
                    # print(f, args, kwargs)
                    if task_id is None or f is None:
                        await self.gather_task(len(self.task_list))
                        self.stop()
                        break
                    # Grabs item and put to task_list
                    self.task_list.append((task_id, f, args, kwargs))
            print('grab jobs = ', len(self.task_list))
            # concurrently run task_list
            await self.gather_task()
            # let another people run
            await asyncio.sleep(self.sleep)

    def run(self):
        asyncio.run(self.consume())


class Future(object):
    def __init__(self, task_id, map_result):
        self._id = task_id
        self._results: dict = map_result

    async def result(self):
        """Wait for all tasks to complete, and return results, preserving order."""
        pending = True
        result = None
        while pending:
            if self._id in self._results:
                result, tb = self._results.pop(self._id)
                if tb is not None:
                    raise Exception(tb)
                break
            await asyncio.sleep(0.005)
        return result

    @property
    def id(self):
        return self._id


class AsyncProcessExecutor(object):
    def __init__(self, max_process=2, max_queue_size=150, task_per_process=50) -> None:
        self.max_process = max_process
        self.last_id = 0
        self.last_process_id = 0
        self.in_queue_map: Dict[int, mp.Queue] = {k: mp.Queue(maxsize=max_queue_size) for k in range(max_process)}
        self.out_queue = mp.Queue()
        self.processes: Dict[int, AsyncProcess] = {
            k: self.create_worker(self.in_queue_map[k], self.out_queue, task_per_process=task_per_process, max_queue=max_queue_size) for k in
            range(max_process)}
        self.results_map: Dict[int, Any] = dict()
        self._loop = asyncio.ensure_future(self.loop())
        self.live = True

    async def __aenter__(self):
        """Enable `async with AsyncProcessExecutor() as Executor` usage."""
        return self

    async def __aexit__(self, *args) -> None:
        """Automatically terminate the pool when falling out of scope."""
        self.terminate()
        await self.shutdown()

    async def loop(self) -> None:
        """Collect results of all tasks"""
        while self.out_queue.qsize() > 0 or self.live == True:
            # pull results into a shared dictionary for later retrieval
            for i in range(self.out_queue.qsize()):
                task_id, res, tb = self.out_queue.get()
                self.results_map[task_id] = (res, tb)
                # print('complete task:', len(self.results_map))
            # let someone else do some work for once
            await asyncio.sleep(0.005)

    @staticmethod
    def create_worker(in_queue, out_queue, task_per_process=50, **kwargs) -> AsyncProcess:
        """Create a worker process attached to the given transmit and receive queues."""
        _process = AsyncProcess(in_queue, out_queue=out_queue, max_task=task_per_process, **kwargs)
        _process.daemon = True
        _process.start()
        return _process

    def next_task_id(self):
        self.last_id += 1
        self.last_id = self.last_id % 10**10
        return self.last_id

    def next_process_id(self):
        return self.last_id % self.max_process

    def submit(self, func: Awaitable, *args: Sequence[Any], **kwargs: Dict[str, Any]) -> Future:
        """Add a new work item to the outgoing queue."""
        task_id = self.next_task_id()
        process_id = self.next_process_id()
        process = self.processes[process_id]
        process.produce(task_id, func, *args, **kwargs)
        return Future(task_id, self.results_map)

    async def map(self, func: Awaitable, iterable: Sequence) -> Sequence:
        """Run a coroutine once for each item in the iterable."""
        ids = [self.submit(func, (item,), {}).id for item in iterable]
        return await self.results(ids)

    async def results(self, ids: Sequence) -> Sequence:
        """Wait for all tasks to complete, and return results, preserving order."""
        pending = set(ids)
        ready: Dict[int, Any] = {}

        while len(pending)>0:
            for tid in pending.copy():
                if tid in self.results_map:
                    result, tb = self.results_map.pop(tid)
                    if tb is not None:
                        raise Exception(tb)
                    ready[tid] = result
                    pending.remove(tid)

            await asyncio.sleep(0.005)

        return [ready[tid] for tid in ids]

    def terminate(self) -> None:
        """Signal to close all process"""
        self.live = False
        for k in self.processes:
            process = self.processes[k]
            process.produce(None, None, [], {})
            process.join()

        for k in self.in_queue_map:
            q = self.in_queue_map[k]
            q.close()

    async def shutdown(self) -> None:
        """Wait for the pool to finish gracefully."""
        await self._loop


async def sample_task(item):
    # process the item
    print(f'consuming item {item}...')
    # simulate i/o operation using sleep
    await asyncio.sleep(random.random())
    print(f'complete item {item}...')
    return item


def test_one_process():
    # read the docs: https://stackoverflow.com/questions/31665328/python-3-multiprocessing-queue-deadlock-when-calling-join-before-the-queue-is-em
    print(os.getpid(), '<- current process')
    in_que = mp.Queue(maxsize=150)
    out_queue = mp.Queue()
    other_process = AsyncProcess(in_que, out_queue=out_queue, name="test")
    other_process.daemon = True
    other_process.start()
    print(other_process.pid)
    time.sleep(2)
    for i in range(150):
        # if raise the number of queue beyond 175 and if an exception is raised in child process then child process will be closed but main process will be deadlock in window platform
        in_que.put_nowait((i, sample_task, (i,), {}))
    # other_process.stop()
    time.sleep(2)
    # other_process.stop()
    in_que.put((0, None, [], {}))
    print("Quitting normally")
    other_process.terminate()
    for i in range(out_queue.qsize()):
        x = out_queue.get()
        print('res = ', x)
    # other_process.join()


if __name__ == "__main__":
    test_one_process()
