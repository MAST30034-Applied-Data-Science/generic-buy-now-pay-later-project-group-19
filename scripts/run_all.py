script_description = """ Runs the whole project pipeline from one script file.
Add all raw data to the `data/tables` folder. 
"""

# Python Libraries
from collections import defaultdict
import logging
import os
import sys
import argparse
import re
# ... TODO: Add to this as necessary

# External Libraries
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession, DataFrame
# ... TODO: Add to this as necessary

# Our Modules
from utilities.log_utilities import logger, file_handler
import utilities.print_utilities as PRINT
import utilities.read_utilities as READ
import utilities.write_utilities as WRITE
from utilities.model_utilities import DEFAULT_MODEL_PATH
from etl_script import etl
from fraud_modelling_script import model_fraud
# from rank_script import rank_merchants
# ... TODO: Add to this as necessary

# Constants (these will modify the behavior of the script)
DEFAULT_INPUT_DATA_PATH = READ.DEFAULT_INPUT_DATA_PATH # where the raw data is
DEFAULT_OUTPUT_DATA_PATH = WRITE.DEFAULT_OUTPUT_DATA_PATH # where the curated data is
DEFAULT_INPUT_MODEL_PATH = DEFAULT_MODEL_PATH
# ... TODO: Add to this as necessary

################################################################################
# Define the ETL Process
################################################################################
def run_all(spark: SparkSession, model_path:str = DEFAULT_INPUT_MODEL_PATH, 
        input_path:str = DEFAULT_INPUT_DATA_PATH, 
        output_path:str = DEFAULT_INPUT_MODEL_PATH
        ) -> 'defaultdict[str, DataFrame|None]':
    """ The whole etl process in one script.

    Args:
        spark (`SparkSession`): Spark session processing the data.
        input_path (str): Path where the raw data is stored.
        output_path (str): Path where the curated/output data will be saved.

    Returns:
        defaultdict[str, `DataFrame` | None]: Output dictionary of datasets
    """


    # run fraud modelling script
    PRINT.print_script_header('running fraud modelling script')
    model_fraud(spark, input_path, model_path)

    # run etl script
    PRINT.print_script_header('running etl script')
    etl(spark, model_path, input_path, output_path) 
    
    # run rank script
    
    return 'SUCCESS'

################################################################################
# Functionality to only run in script mode
################################################################################
if __name__ == '__main__':

    ############################################################################
    # Get script parameter(s)
    ############################################################################
    # define the parser
    parser = argparse.ArgumentParser(description=script_description)

    # overwrite the debugging flag
    parser.add_argument('-d', '--debug', '--debugging',
        default=False,
        help='Whether to print debug statements.',
        action='store_true')

    # data input folder
    parser.add_argument('-i', '--input', 
        default=DEFAULT_INPUT_DATA_PATH,
        help='the folder where the data is stored.')

    # model input folder
    parser.add_argument('-m', '--model', 
        default=DEFAULT_INPUT_MODEL_PATH,
        help='the folder where the model is stored.')

    # data output folder
    parser.add_argument('-o', '--output', 
        default=DEFAULT_OUTPUT_DATA_PATH,
        help='the folder where the results are stored. Subdirectories may be created.')

    # ... TODO: Add to this as necessary

    args = parser.parse_args()
    
    # apply the logger level to logger
    if args.debug:
        logger.setLevel(logging.DEBUG)
        file_handler.setLevel(logging.DEBUG)
        logger.addHandler(file_handler)
    else: 
        logger.setLevel(logging.INFO)
        file_handler.setLevel(logging.INFO)
        logger.addHandler(file_handler)

    # print args to debug
    logger.debug(f'arguments: \n{args}')

    ############################################################################
    # Start a spark session
    ############################################################################
    PRINT.print_script_header('creating the spark session')
    spark = (
        SparkSession.builder.appName("MAST30034 Project 2")
        .config("spark.sql.repl.eagerEval.enabled", True) 
        .config("spark.sql.parquet.cacheMetadata", "true")
        .config("spark.sql.session.timeZone", "Etc/UTC")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel('WARN')

    ############################################################################
    # Run the entire project pipeline in one go
    ############################################################################
    output = run_all(spark, args.model, args.input, args.output)    

    logger.info('Run All Scripts Successfully!')