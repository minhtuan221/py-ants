# py-Ants
Provide executor pool for simultaneous processing task/message without  the problem of arbitrary order.
refer link: https://systeminterview.com/

## Synchronization

### Locker
Locks are implemented using 2 separate counter, a ticket counter and a access counter. Each thread trying to acquire lock request a ticket by incrementing the ticket counter. The lock is granted to the thread if and only if the value of the ticket counter equals to the value of access counter. On release of the lock, the thread increments the access counter then make the next waiting thread grant the lock access.

### Barrier
A barrier is a type of synchronization method. A barrier for a group of threads or processes in the source code means any thread/process must stop at this point and cannot proceed until all other threads/processes reach this barrier.
Barrier has a global counter. Each thread entrance into the barrier increment the counter. When the counter reach to the value that is divisible by the number of thread in computer cluster, all thread are reach the barrier and allow to continue. To distinguish the multiple generations of one barriers, a non-shard counter is maintain and increment if the barrier is reached or executed.

### Mutex 
Mutex provides one person to access a single resource at a time, others must wait in a queue. Once this person is done, the guy next in the queue acquire the resource.
So access is serial, one guy after other. Too aggressive.

### Spinlock
Spinlock is an aggressive mutex. In mutex, if you find that resource is locked by someone else, you (the thread/process) switch the context and start to wait (non-blocking).
Whereas spinlocks do not switch context and keep spinning. As soon as resource is free, they go and grab it. In this process of spinning, they consume many CPU cycles. Also, on a uni-processor machine they are useless and perform very badly (do I need to explain that?).
```
 while(something != TRUE ){};
 // it happend
 move_on();
```

### Semaphore
Semaphore are useful if multiple instances (N) of a resource is to be shared among a set of users. As soon as all N resources are acquired, any new requester has to wait. Since there is no single lock to hold, there is as such no ownership of a semaphore.

A semaphore manages an internal counter which is decremented by each acquire() call and incremented by each release() call. The counter can never go below zero; when acquire() finds that it is zero, it blocks, waiting until some task calls release().

The optional value argument gives the initial value for the internal counter (1 by default). If the given value is less than 0 a ValueError is raised.
Semaphores are often used to guard resources with limited capacity, for example, a database server. In any situation where the size of the resource is fixed, you should use a bounded semaphore. Before spawning any worker threads, your main thread would initialize the semaphore:

### Event Objects
This is one of the simplest mechanisms for communication between threads: one thread signals an event and other threads wait for it.

An event object manages an internal flag that can be set to true with the set() method and reset to false with the clear() method. The wait() method blocks until the flag is true.

### Condition Objects
A condition variable is always associated with some kind of lock; this can be passed in or one will be created by default. Passing one in is useful when several condition variables must share the same lock. The lock is part of the condition object: you don’t have to track it separately.

A condition variable obeys the context management protocol: using the with statement acquires the associated lock for the duration of the enclosed block. The acquire() and release() methods also call the corresponding methods of the associated lock.

Other methods must be called with the associated lock held. The wait() method releases the lock, and then blocks until another thread awakens it by calling notify() or notify_all(). Once awakened, wait() re-acquires the lock and returns. It is also possible to specify a timeout.

The notify() method wakes up one of the threads waiting for the condition variable, if any are waiting. The notify_all() method wakes up all threads waiting for the condition variable.

Note: the notify() and notify_all() methods don’t release the lock; this means that the thread or threads awakened will not return from their wait() call immediately, but only when the thread that called notify() or notify_all() finally relinquishes ownership of the lock.
```python
# Consume one item
with cv:
    while not an_item_is_available():
        cv.wait()
    get_an_available_item()

# Produce one item
with cv:
    make_an_item_available()
    cv.notify()
```
The while loop checking for the application’s condition is necessary because wait() can return after an arbitrary long time, and the condition which prompted the notify() call may no longer hold true. This is inherent to multi-threaded programming. The wait_for() method can be used to automate the condition checking, and eases the computation of timeouts:
```
# Consume an item
with cv:
    cv.wait_for(an_item_is_available)
    get_an_available_item()
```
### Readers–writer lock
A readers–writer (RW) or shared-exclusive lock (also known as a multiple readers/single-writer lock, a multi-reader lock, a push lock, or an MRSW lock) is a synchronization primitive that solves one of the readers–writers problems. An RW lock allows concurrent access for read-only operations, while write operations require exclusive access. This means that multiple threads can read the data in parallel but an exclusive lock is needed for writing or modifying data. When a writer is writing the data, all other writers or readers will be blocked until the writer is finished writing. A common use might be to control access to a data structure in memory that cannot be updated atomically and is invalid (and should not be read by another thread) until the update is complete.

### Read-copy-update
Read-copy-update (RCU) is a synchronization mechanism based on mutual exclusion. It is used when performance of reads is crucial and is an example of space–time tradeoff, enabling fast operations at the cost of more space.

Read-copy-update allows multiple threads to efficiently read from shared memory by deferring updates after pre-existing reads to a later time while simultaneously marking the data, ensuring new readers will read the updated data. This makes all readers proceed as if there were no synchronization involved, hence they will be fast, but also making updates more difficult.

### Referrence:

- python 3 - GIL: https://cloudsek.com/how-do-you-achieve-concurrency-with-python-threads/

- python 3 - GIL: https://tenthousandmeters.com/blog/python-behind-the-scenes-13-the-gil-and-its-effects-on-python-multithreading/

- python 3 - GIL, convoy effect: https://github.com/python/cpython/issues/52194

- mutiple threading in Java: https://www.digitalocean.com/community/tutorials/multithreading-in-java
