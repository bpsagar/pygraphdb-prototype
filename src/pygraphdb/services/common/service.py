__author__ = 'Sagar'
from queue import Queue
from threading import Thread, Lock
import logging
from pygraphdb.threading.namedthread import NamedThread

class Service(NamedThread):
    COMMUNICATION_INSTANCE = None
    def __init__(self, **config):
        super(Service, self).__init__()
        self._running = False
        self._ready = False
        self._service_name = config.get('service_name', self.__class__.__name__)
        self._logger = logging.getLogger(self._service_name)
        self._config = config
        self._children = []
        self._parent = config.get('parent', None)
        if not self._parent is None:
            self._parent.add_child_service(self)
        self.validate_config(config)
        self.construct(config)

    def is_ready(self):
        return self._ready

    def get_parent(self):
        return self._parent

    def add_child_service(self, child_service):
        self._children.append(child_service)

    def validate_config(self, config={}):
        pass

    def construct(self, config={}):
        pass

    def init(self):
        pass

    def deinit(self):
        pass

    def startup(self):
        self.start()

    def shutdown(self):
        self._running = False
        self.join()

    def run(self):
        # Initialize
        self._running = True
        self._logger.info("Initializing...")
        self.init()
        self._ready = True

        self._logger.info("Init complete. Now running...")
        # Main loop
        while self._running:
            try:
                status = self.do_work()
                if not status:
                    break
            except TimeoutError:
                continue
            #except Exception as e:
            #    self._logger.fatal('Unhandled exception [%s] received! Service will shutdown!', e.__class__.__name__)
            #    self._logger.debug(str(e))
            #    break

        self._running = False
        for child_service in self._children:
            child_service.shutdown()
        # De-Initialize
        self._logger.info("De-initializing...")
        self.deinit()
        self._logger.info("Run Complete!")

    def send(self, target_name, target_service, message):
        Service.COMMUNICATION_INSTANCE.send(self._service_name, target_name, target_service, message)