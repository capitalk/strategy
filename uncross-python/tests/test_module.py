import sys
import logging

logger = logging.getLogger('test')

def log_test3():
  x = range(10)
  for i in x:
      logger.info("log_test3 %d", i)

