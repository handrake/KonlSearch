import threading
import collections


class NamedLock:
    def __init__(self):
        self.lock = threading.Lock()
        self.locks = collections.defaultdict(threading.Lock)

    def acquire(self, name):
        with self.lock:
            lock = self.locks[name]
        lock.acquire()

    def release(self, name):
        with self.lock:
            lock = self.locks[name]
        lock.release()
