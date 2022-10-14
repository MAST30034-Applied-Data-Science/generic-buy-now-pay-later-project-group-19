script_description = """ Generates the fraud probability regression model.
"""

# Python Libraries
import logging
import argparse
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
DEFAULT_INPUT_DATA_PATH = READ.DEFAULT_INPUT_DATA_PATH # where the raw data is
DEFAULT_OUTPUT_MODEL_PATH = MODEL.DEFAULT_MODEL_PATH
# ... TODO: Add to this as necessary

################################################################################
# Define the ETL Process
################################################################################
def model_fraud(spark: SparkSession, input_path:str = DEFAULT_INPUT_DATA_PATH, 
        model_path:str = DEFAULT_OUTPUT_MODEL_PATH) -> LR:
    """ TODO: commenting.

    Args:
        spark (`SparkSession`): Spark session processing the data.
        input_path (str): Path where the raw data is stored.
        model_path (str): Path where resulting fraud model will be saved.

    Returns:
        `LinearRegression`: Output fraud model.
    """

    # read in the datasets
    PRINT.print_script_header('reading in the raw transactions')
    
    transaction_df = READ.read_all_transactions(spark, input_path)
    logger.info(f'Read {transaction_df.count()} transaction entries')
    logger.debug(transaction_df.head(5))

    consumer_fraud_df = READ.read_consumer_fraud(spark, input_path)
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
        PRINT.print_script_header('saving the data')
        WRITE.write_model(fraud_lr, model_path)

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
    output = model_fraud(spark, args.input, args.output)    

    logger.info('Fraud Modelling Complete!')