script_description = """ This script file should be used as a template for any script files you find yourself using.
Use this comment to describe what the script does.
Work through the TODOs for each script you make and it should be perfect.
"""

# Python Libraries
import os
import sys
import argparse
import inspect
import re
# ... TODO: Add to this as necessary

# External Libraries
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
# ... TODO: Add to this as necessary

# Constants (these will modify the behavior of the script)
DEBUGGING = False
SPARK = False
# ... TODO: Add to this as necessary

################################################################################
# %% Debugging
################################################################################
def debug(msg: str, type: str = 'DEBUG'):
    """ Prints a debug message to the console.

    Args:
        msg (str): Message
        type (str, optional): Type of debug statemend (e.g. `'DEBUG'` or `'ERROR'`). 
            Defaults to `'DEBUG'`.
    """

    if not DEBUGGING: return
    # reference: https://stackoverflow.com/questions/24438976/debugging-get-filename-and-line-number-from-which-a-function-is-called
    caller = inspect.getframeinfo(inspect.stack()[1][0])
    print(f'<{type}@{caller.filename}:{caller.lineno}> {msg}')

################################################################################
# %% Get script parameter(s)
################################################################################

parser = argparse.ArgumentParser(description=script_description)

# overwrite the debugging flag
parser.add_argument('-d', '--debug', '--debugging',
    default=False,
    help='Whether to print debug statements.',
    action='store_true')

# data input
parser.add_argument('-i', '--input', 
    default='../data/tables',
    help='the folder where the data is stored.')

# output folder
parser.add_argument('-o', '--output', 
    default='../data/curated',
    help='the folder where the results are stored. Subdirectories may be created.')

# ... TODO: Add to this as necessary

args = parser.parse_args()
DEBUGGING = args.debug

# Access the given arguments with
debug(args.input)
debug(args.output)
debug(args)

################################################################################
# %% Start a spark session
################################################################################
spark = None
if SPARK:
    spark = (
        SparkSession.builder.appName("MAST30034 Project 2")
        .config("spark.sql.repl.eagerEval.enabled", True) 
        .config("spark.sql.parquet.cacheMetadata", "true")
        .config("spark.sql.session.timeZone", "Etc/UTC")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )