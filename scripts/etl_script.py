script_description = """ Runs the whole ETL pipeline from one script file.
Add all raw data to the `data/tables` folder. 
Data not in this folder will not be read by this script unless explicitly defined.
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
# ... TODO: Add to this as necessary

# Constants (these will modify the behavior of the script)
DEFAULT_INPUT_DATA_PATH = READ.DEFAULT_INPUT_DATA_PATH # where the raw data is
DEFAULT_OUTPUT_DATA_PATH = WRITE.DEFAULT_OUTPUT_DATA_PATH # where the curated data is
DEFAULT_INPUT_MODEL_PATH = DEFAULT_MODEL_PATH
# ... TODO: Add to this as necessary

################################################################################
# Define the ETL Process
################################################################################
def etl(spark: SparkSession, model_path:str = DEFAULT_INPUT_MODEL_PATH, 
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

    import utilities.clean_utilities as CLEAN
    import utilities.agg_utilities as AGG

    # read in the datasets
    PRINT.print_script_header('reading in the raw datasets')
    data_dict = READ.read_data(spark, input_path)

    logger.debug(f'Added datasets: {data_dict.keys()}')

    # summaries of the datasets
    PRINT.print_script_header('summary information')
    PRINT.print_dataset_summary(data_dict)

    # clean the data
    PRINT.print_script_header('cleaning the data')
    CLEAN.clean_data(spark, data_dict)
    PRINT.print_dataset_summary(data_dict, 
        ['transactions', 'merchants', 'merchant_tags'])

    # compute aggregate tables
    PRINT.print_script_header('aggregating the data')
    AGG.compute_aggregates(spark, data_dict)
    PRINT.print_dataset_summary(data_dict, 
        ['merchant_sales', 'customer_accounts', 'customer_transactions'])

    logger.info('I will now save all the data unless the output path is None.')
    
    if output_path is not None:
        PRINT.print_script_header('saving the data')
        WRITE.write_data(data_dict, output_path)

    return data_dict

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
    # Run the ETL Process
    ############################################################################
    output = etl(spark, args.model, args.input, args.output)    

    logger.info('ETL Complete!')