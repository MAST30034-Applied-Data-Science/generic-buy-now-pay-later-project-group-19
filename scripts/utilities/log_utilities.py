""" This utility provides the setup for accessing the logs.
"""
import sys
import os
import logging
from logging import handlers, Formatter

# determine the name of the log file.
homescript_dir = sys.argv[0]
homescript = homescript_dir.split('/')[-1].split('.')[0]
filename = f'./log/{homescript}.log'

# create the log folder if necessary
if not os.path.exists('./log'):
    os.mkdir('./log')

# create the logger
logging.basicConfig(
    format='<%(asctime)s | %(filename)s:%(lineno)s | %(levelname)s> %(message)s',
    datefmt='%H:%M:%S',
    level=logging.ERROR
)
logging.info("Start Logging")
logger = logging.getLogger('etl_logger')

# create a file handler
file_handler = logging.FileHandler(filename, mode='w', delay = True)
file_handler.setFormatter(
    Formatter(
        fmt='<%(asctime)s | %(filename)s:%(lineno)s | %(levelname)s> %(message)s',
        datefmt='%H:%M:%S',
    )
)

# add the file handler to the logger
logger.addHandler(file_handler)