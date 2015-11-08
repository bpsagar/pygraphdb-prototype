from pygraphdb.process.workerprocess import WorkerProcess
import logging
import time
import sys

logging.basicConfig(format='%(asctime)s\t%(levelname)-10s\t%(threadName)-32s\t%(name)-32s\t%(message)s', level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')

worker = WorkerProcess()
worker.init(sys.argv[1])
worker.run()

time.sleep(600000)
worker.stop()