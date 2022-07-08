import asyncio
import random
import threading
import time
import typing as t
import queue


async def worker(name, queue):
    while True:
        # Get a "work item" out of the queue.
        sleep_for = await queue.get()

        # Sleep for the "sleep_for" seconds.
        await asyncio.sleep(sleep_for)

        # Notify the queue that the "work item" has been processed.
        queue.task_done()

        print(f'{name} has slept for {sleep_for:.2f} seconds')


async def main():
    # Create a queue that we will use to store our "workload".
    queue = asyncio.Queue()

    # Generate random timings and put them into the queue.
    total_sleep_time = 0
    for _ in range(20):
        sleep_for = random.uniform(0.05, 1.0)
        total_sleep_time += sleep_for
        queue.put_nowait(sleep_for)

    # Create three worker tasks to process the queue concurrently.
    tasks = []
    for i in range(3):
        task = asyncio.create_task(worker(f'worker-{i}', queue))
        tasks.append(task)

    # Wait until the queue is fully processed.
    started_at = time.monotonic()
    await queue.join()
    total_slept_for = time.monotonic() - started_at

    # Cancel our worker tasks.
    for task in tasks:
        task.cancel()
    # Wait until all worker tasks are cancelled.
    await asyncio.gather(*tasks, return_exceptions=True)

    print('====')
    print(f'3 workers slept in parallel for {total_slept_for:.2f} seconds')
    print(f'total expected sleep time: {total_sleep_time:.2f} seconds')


async def Worker(name, in_queue: asyncio.Queue, delay=0.01):
    while True:
        # Get a "work item" out of the queue.
        try:
            f = in_queue.get_nowait()
        except asyncio.QueueEmpty:
            # Sleep for the "sleep_for" seconds.
            await asyncio.sleep(delay)
            continue

        result = await f

        # Notify the queue that the "work item" has been processed.
        in_queue.task_done()


class AsyncPool(object):

    def __init__(self, loop=None, max_workers:int = 2, delay=0.1):
        self.loop = asyncio.new_event_loop()
        self.max_workers = max_workers
        self.workers = []
        self.queue_list: t.List[asyncio.Queue] = []
        self.delay = delay
        self.init_worker()
        self.pool_thread = None

    def init_worker(self):
        self.queue_list = [asyncio.Queue() for _ in range(self.max_workers)]
        for i in range(self.max_workers):
            task = self.loop.create_task(Worker(f'worker-{i}', self.queue_list[i], self.delay))
            self.workers.append(task)

    def hash_to_choose_worker(self, key) -> int:
        if key is None:
            k = random.randint(0, self.max_workers - 1)
        else:
            try:
                k = hash(key) % self.max_workers
            except TypeError:
                k = random.randint(0, self.max_workers - 1)
        return k

    def submit(self, f: t.Coroutine):
        return self.submit_id(None, f)

    def submit_id(self, key: t.Union[int, str, float, None], f: t.Coroutine):
        # hash the key
        worker_id = self.hash_to_choose_worker(key)

        # assign to worker queue
        w = self.queue_list[worker_id]
        # print(key, 'choose queue =>', worker_id)

        # add function to queue
        while True:
            # this will block until it can put function into queue
            try:
                w.put_nowait(f)
                break
            except asyncio.QueueFull:
                time.sleep(0.1)
                continue

    def wait(self):
        self.pool_thread = threading.Thread(target=self.loop.run_until_complete,args=[self.shutdown()],daemon=True)
        self.pool_thread.start()

    async def shutdown(self):
        # Wait until the queue is fully processed.
        for q in self.queue_list:
            await q.join()
        # Cancel our worker tasks.
        # for task in self.workers:
        #     task.cancel()
        await asyncio.gather(*self.workers, return_exceptions=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.shutdown()


def wait_pool(pool):
    pool.loop.run_until_complete(pool.shutdown())


def async_run_as_thread():
    pool = AsyncPool(max_workers=2)
    pool_thread = threading.Thread(target=wait_pool,args=[pool],daemon=True)
    pool_thread.start()
    return pool


async def async_task(i):
    s = random.uniform(0.01, 0.1)
    await asyncio.sleep(s)
    print('complete task', i)


async def test_async_pool(w=2):
    pool = AsyncPool(max_workers=w)
    for i in range(100):
        f = async_task(i)
        pool.submit_id(i % 2**8, f)
    await pool.shutdown()


if __name__ == "__main__":
    pool = AsyncPool()
    pool.wait()
    print('we can run here')
    for i in range(100):
        f = async_task(i)
        pool.submit_id(i % 2**8, f)
    time.sleep(10)
    print('timeout')
