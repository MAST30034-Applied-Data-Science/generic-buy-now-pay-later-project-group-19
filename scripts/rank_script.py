script_description = """ Generates the fraud probability regression model.
"""

# Python Libraries
import logging
import argparse
from collections import defaultdict
# ... TODO: Add to this as necessary

# External Libraries
from pyspark.sql import SparkSession, DataFrame
from pyspark.ml.regression import LinearRegression as LR
# ... TODO: Add to this as necessary

# Our Modules
from utilities.log_utilities import logger, file_handler
import utilities.print_utilities as PRINT
import utilities.read_utilities as READ
import utilities.agg_utilities as AGG
import utilities.model_utilities as MODEL
import utilities.write_utilities as WRITE
# ... TODO: Add to this as necessary

# Constants (these will modify the behavior of the script)
DEFAULT_INPUT_DATA_PATH = WRITE.DEFAULT_OUTPUT_PATH # where the raw data is
DEFAULT_OUTPUT_RANK_PATH = WRITE.DEFAULT_RANKING_PATH
# ... TODO: Add to this as necessary

################################################################################
# Define the ETL Process
################################################################################
def rank_merchants(input_path:str = DEFAULT_INPUT_DATA_PATH, 
        rank_path:str = DEFAULT_OUTPUT_RANK_PATH) -> 'defaultdict[str]':
    """ TODO: commenting.

    Args:
        input_path (str): Path where the curated data is stored.
        rank_path (str): Path where resulting rankings will be saved.

    Returns:
        `LinearRegression`: Output fraud model.
    """

    # read in the datasets
    PRINT.print_script_header('reading in the raw transactions')
    
    merchant_statistics_df = READ.read_merchant_statistics(input_path)
    logger.info(f'Read {merchant_statistics_df.count()} merchant entries')
    logger.debug(merchant_statistics_df.head(5))

    segment_df = READ.read_segments(input_path)
    logger.info(f'Read {consumer_fraud_df.count()} consumer fraud entries')
    logger.debug(consumer_fraud_df.head(5))

    # compute aggregate daily table
    PRINT.print_script_header('aggregating the transactions by day by merchant')
    daily_consumer_fraud_df = AGG.compute_known_consumer_fraud(
        spark, transaction_df, consumer_fraud_df
    )

    PRINT.print_script_header('generate the linear regression')
    fraud_lr = MODEL.generate_fraud_model(daily_consumer_fraud_df)

    logger.info('I will now save the model unless the output path is None.')
    
    if model_path is not None:
        PRINT.print_script_header('saving the rankings')
        WRITE.write_ranking(fraud_lr, rank_path)

    return fraud_lr

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

    # data output folder
    parser.add_argument('-o', '--output', 
        default=DEFAULT_OUTPUT_MODEL_PATH,
        help='the folder where the model is stored. Subdirectories may be created.')

    # Parse arguments as necessary
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
    output = rank_merchants(args.input, args.output)    

    logger.info('Fraud Modelling Complete!')