
import logging

def create_logger(name, console_level = None, file_name = None, file_level = logging.FATAL):
  assert file_name or console_level 
  logger = logging.getLogger(name)
  logger.setLevel(min(console_level, file_level))
  formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
  if file_name:
    fh = logging.FileHandler(file_name)
    fh.setLevel(file_level)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
  if console_level:
    ch = logging.StreamHandler()
    ch.setLevel(console_level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
  return logger
