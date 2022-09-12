script_description = """ Runs the whole ETL pipeline from one script file.
Add all raw data to the `data/tables` folder. 
Data not in this folder will not be read by this script unless explicitly defined.
TODO: Add documentation of the steps here.
"""

# Python Libraries
from collections import defaultdict
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
from modules.log_utilities import logger
from modules.printing_utilities import print_script_header
import modules.reading_utilities as READ
import modules.cleaning_utilities as CLEAN
# ... TODO: Add to this as necessary

# Constants (these will modify the behavior of the script)
SPARK = True
DEFAULT_INPUT_PATH = './data/tables' # where the raw data is
DEFAULT_OUTPUT_PATH = './data/curated' # where the curated data is
# ... TODO: Add to this as necessary

################################################################################
# Define the ETL Process
################################################################################
def etl(spark: SparkSession, input_path:str, 
        output_path:str) -> 'defaultdict[str, DataFrame|None]':
    """ The whole etl process in one script.

    Args:
        spark (`SparkSession`): Spark session processing the data.
        input_path (str): Path where the raw data is stored.
        output_path (str): Path where the curated/output data will be saved.

    Returns:
        defaultdict[str, `DataFrame` | None]: Output dictionary of datasets
    """

    # read in the datasets
    print_script_header('reading in the raw datasets')
    data_dict = READ.read_data(spark, args.input)

    logger.debug(data_dict.keys())

    # summaries of the datasets
    for dataset_name, df in data_dict.items():
        logger.debug(df.show(20))
        logger.info(f'Check missing values in the {dataset_name} dataset')
        logger.info(f'{CLEAN.check_missing_values(df)}')

    # remove outliers in transactions
    logger.info('Removing transaction outliers')
    data_dict['transactions'] = CLEAN.remove_transaction_outliers(
        data_dict['transactions']
    )

    # extract merchant tags
    logger.info('Extracting merchant tags')
    data_dict['merchants'] = CLEAN.extract_tags(data_dict['merchants'])

    

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

    # data input
    parser.add_argument('-i', '--input', 
        default=DEFAULT_INPUT_PATH,
        help='the folder where the data is stored.')

    # output folder
    parser.add_argument('-o', '--output', 
        default=DEFAULT_OUTPUT_PATH,
        help='the folder where the results are stored. Subdirectories may be created.')

    # ... TODO: Add to this as necessary

    args = parser.parse_args()
    
    # print args to debug
    logger.debug(f'arguments: \n{args}')

    ############################################################################
    # Start a spark session
    ############################################################################
    print_script_header('creating the spark session')
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
    etl(spark, args.input, args.output)