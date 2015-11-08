__author__ = 'sagar'

from threading import Thread, Lock

class NamedThread(Thread):
    thread_names = {}
    lock = Lock()

    @classmethod
    def get_thread_name(cls, obj):
        NamedThread.lock.acquire()
        count = NamedThread.thread_names.get(obj.__class__, -1)
        count += 1
        NamedThread.thread_names[obj.__class__] = count
        NamedThread.lock.release()
        return '%s-%d' % (obj.__class__.__name__, count)

    def __init__(self):
        super(NamedThread, self).__init__(name=NamedThread.get_thread_name(self))
