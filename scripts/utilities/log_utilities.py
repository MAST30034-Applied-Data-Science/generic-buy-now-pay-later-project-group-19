""" This utility provides the setup for accessing the logs.
"""

import sys
import os
import logging
from logging import handlers

homescript_dir = sys.argv[0]
homescript = homescript_dir.split('/')[-1].split('.')[0]
filename = f'./log/{homescript}.log'

logging.basicConfig(
    format='<%(levelname)s @ %(asctime)s | %(filename)s:%(lineno)s> %(message)s',
    datefmt='%H:%M:%S',
    level=logging.ERROR
)

logging.info("Start Logging")

logger = logging.getLogger('etl_logger')
logger.setLevel(logging.DEBUG)

# create a file handler and set level to INFO
file_handler = logging.FileHandler(filename, mode='w')
file_handler.setLevel(logging.DEBUG)
logger.addHandler(file_handler)