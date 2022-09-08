""" This utility provides the setup for accessing the logs.
"""

import sys
import os
import logging
from logging import handlers

homescript_dir = sys.argv[0]
homescript = homescript_dir.split('/')[-1].split('.')[0]
filename = f'./log/{homescript}.log'

logging.basicConfig(filename=filename,
                    filemode='a',
        # format='<%(asctime)s|%(filename)s:%(funcName)s:%(lineno)d|%(levelname)s> %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

logging.info("Start Logging")

logger = logging.getLogger('logger')

# add the file output handler
should_roll_over = os.path.isfile(filename)
handler = handlers.RotatingFileHandler(filename, mode='w', backupCount=5)
if should_roll_over:  # log already exists, roll over!
    handler.doRollover()

logger.addHandler(handler)

# add the output handler
logger.addHandler(logging.StreamHandler(sys.stdout))