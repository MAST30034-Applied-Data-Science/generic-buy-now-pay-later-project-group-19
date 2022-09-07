script_description = """ Runs the whole ETL pipeline from one script file.
TODO: Add documentation of the steps here.
"""

# Python Libraries
import os
import sys
import argparse
import re
# ... TODO: Add to this as necessary

# External Libraries
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
# ... TODO: Add to this as necessary

# Our Modules
import modules.logging as logging
import modules.reading as READ
import modules.util as util
# ... TODO: Add to this as necessary

# Constants (these will modify the behavior of the script)
SPARK = True
DEFAULT_RAW_DATA_PATH = './data/tables'
DEFAULT_CURATED_DATA_PATH = './data/curated'
# ... TODO: Add to this as necessary

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
    default=DEFAULT_RAW_DATA_PATH,
    help='the folder where the data is stored.')

# output folder
parser.add_argument('-o', '--output', 
    default=DEFAULT_CURATED_DATA_PATH,
    help='the folder where the results are stored. Subdirectories may be created.')

# ... TODO: Add to this as necessary

args = parser.parse_args()
LOG = logging.Logger(args.debug)

# Access the given arguments with
LOG.print_script_header('reading the script\'s arguments')
LOG.debug(args.input)
LOG.debug(args.output)
LOG.debug(args)

################################################################################
# %% Start a spark session
################################################################################
spark = None
if SPARK:
    LOG.print_script_header('creating the spark session')
    spark = (
        SparkSession.builder.appName("MAST30034 Project 2")
        .config("spark.sql.repl.eagerEval.enabled", True) 
        .config("spark.sql.parquet.cacheMetadata", "true")
        .config("spark.sql.session.timeZone", "Etc/UTC")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

################################################################################
# %% Read in the datasets
################################################################################
LOG.print_script_header('reading in the raw datasets')
consumer_data = READ.read_consumers(spark, data_path=args.input)
transaction_data = READ.read_transactions(spark, data_path=args.input)
merchant_data = READ.read_merchants(spark, data_path=args.input)
user_detail_data = READ.read_consumer_user_mappings(spark, data_path=args.input)

################################################################################
# %% Preprocessing datasets
################################################################################
LOG.print_script_header('processing transaction datasets')
print("Check missing values in the transaction dataset")
print(util.check_missing_values(transaction_data))
transaction_data = util.remove_transaction_outliers(transaction_data)

LOG.print_script_header('processing merchant dataset')
print("Check missing values in the merchants dataset")
print(util.check_missing_values(merchant_data))
merchant_data = util.extract_tags(merchant_data)
print("Cleaning merchants tags")

LOG.print_script_header('processing consumer datasets')

################################################################################
# %% Aggregating datasets
################################################################################
LOG.print_script_header('aggregating datasets')

# Generate data which summarizes merchants' sales
print("Aggregating merchants' sale by merchant abn and date")
merchant_sales = (transaction_data.groupby('merchant_abn', 'order_datetime')
                             .agg({'dollar_value':'sum',
                                   'order_id':'count'})
                             .withColumnRenamed('sum(dollar_value)', 'sales_revenue')
                             .withColumnRenamed('count(order_id)', 'no_orders'))

merchant_sales.write.parquet(DEFAULT_CURATED_DATA_PATH + "/merchant_sales.parquet")

# Generate data which summarizes customers spendings
print("Aggregating customers' purchases by user id and date")
customer_purchases = (transaction_data.groupby('user_id', 'order_datetime')
                                 .agg({'dollar_value':'sum',
                                       'order_id':'count'})
                                 .withColumnRenamed('sum(dollar_value)', 'dollar_spent')
                                 .withColumnRenamed('count(order_id)', 'no_orders'))

customer_purchases.write.parquet(DEFAULT_CURATED_DATA_PATH + "/customer_purchase_behaviour.parquet")


print("Aggregating customers' purchases by state, postcode and date")
# Join transaction data with user detail data
customer_transaction = (transaction_data.join(
                            user_detail_data, 
                            transaction_data.user_id == user_detail_data.user_id
                        ).drop(
                            user_detail_data.user_id
                        ))

# Join the data with consumer data
customer_transaction = (customer_transaction.join(consumer_data, customer_transaction.consumer_id == consumer_data.consumer_id)
                                            .drop(consumer_data.consumer_id)
                                            .select(transaction_data['*'], 
                                                    consumer_data.postcode, 
                                                    consumer_data.state, 
                                                    consumer_data.gender))

# Aggregate by state -> postcode -> date
sales_by_region = (customer_transaction.groupby('state', 'postcode', 'order_datetime')
                                       .agg({'dollar_value':'sum',
                                             'order_id':'count'})
                                       .withColumnRenamed('sum(dollar_value)', 'dollar_spent')
                                       .withColumnRenamed('count(order_id)', 'no_orders'))

sales_by_region.write.parquet(DEFAULT_CURATED_DATA_PATH + "/sales_by_region.parquet")


