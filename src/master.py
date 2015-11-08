from pygraphdb.process.masterprocess import MasterProcess
import logging
import time
import sys

logging.basicConfig(format='%(asctime)s\t%(levelname)-10s\t%(threadName)-32s\t%(name)-32s\t%(message)s', level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')
master = MasterProcess()
master.init(sys.argv[1])
master.run()

time.sleep(600000)
master.stop()