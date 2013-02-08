import sys
import logging
from test_logging_helpers import create_logger
from test_module import log_test3

logger = create_logger("test", 
  console_level = logging.DEBUG, 
  file_name = "test.log", 
  file_level = logging.DEBUG)

def log_test1():
  # TODO: Figure out why zmq sockets hang on exit
  # atexit.register(strategy.close_all)
  x = range(10)
  for i in x:
      #print i
      logger.info("log_test1 %d", i)

def log_test2():
    logging.basicConfig(filename="test2.log" , level=logging.DEBUG)
    logging.debug("Debug level")
    logging.info("Info level")
    logging.warning("Warning level")

if __name__ == '__main__':
    log_test1()
    #log_test2()
    log_test3()
