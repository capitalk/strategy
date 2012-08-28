
import logging

log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def create_logger(name, console_handler = None, console_level = None, 
    file_name = None, file_level = logging.FATAL):
  assert file_name or console_level 
  logger = logging.getLogger(name)
  levels = []
  old_level = logger.getEffectiveLevel()
  if old_level != logging.NOTSET:
    levels.append(old_level)
  if console_level is not None:
    levels.append(console_level)
  if file_level is not None:
    levels.append(file_level)
  assert (len(levels)) > 0
  min_level = reduce(min, levels)
  logger.setLevel(min_level)
  if file_name:
    fh = logging.FileHandler(file_name)
    fh.setLevel(file_level)
    fh.setFormatter(log_formatter)
    logger.addHandler(fh)
  if console_level:
    if console_handler is None: 
      console_handler = logging.StreamHandler()
    
    console_handler.setLevel(console_level)
    console_handler.setFormatter(log_formatter)
    logger.addHandler(console_handler)
  return logger
