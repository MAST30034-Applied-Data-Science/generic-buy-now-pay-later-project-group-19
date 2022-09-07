''' Provides functions to read the datasets.
'''

from collections import defaultdict
import os
import re
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DEFAULT_DATA_PATH = '../data/tables'

# regex queries for finding the relevant datasets
RE_CONSUMERS = r'tbl_consumer.*\.csv'
RE_CONSUMER_USER_MAPPINGS = r'consumer_user_details.*\.parquet'
RE_TRANSACTIONS = r'transactions_\d+_\d+_snapshot'
RE_MERCHANTS = r'tbl_merchants.*\.parquet'

def union_or_create(df: DataFrame, new_df: DataFrame) -> DataFrame:
    """ Either return a new dataset or append the new dataset to the old one.

    Args:
        df (`DataFrame` | None): The original `DataFrame`.
        new_df (`DataFrame`): The new `DataFrame`

    Returns:
        `DataFrame`: Output `DataFrame`
    """
    if df is None:
        return new_df
    else:
        return df.union(new_df)

    """ Read in all the relevant datasets placed in the one raw/tables folder.
    This makes assumptions about the naming schemes of each table,
    as per the `regex` queries defined at the top of this module.

    Args:
        spark (`SparkSession`): Spark session reading the data.
        data_path (str, optional): Path where the raw data is stored. 
            Defaults to `../data/path` (the relative location from script locations).

    Returns:
        `
    """

def read_data(spark: SparkSession, 
        data_path: str = DEFAULT_DATA_PATH) -> 'defaultdict[str, DataFrame|None]':
    """ Read in all the relevant datasets placed in the one raw/tables folder.
    This makes assumptions about the naming schemes and file formats of each dataset,
    as per the `regex` queries defined at the top of this module.

    Args:
        spark (`SparkSession`): Spark session reading the data.
        data_path (str, optional): Path where the raw data is stored. 
            Defaults to `../data/path` (the relative location from script locations).

    Returns:
        defaultdict[str, `DataFrame` | None]: Output dictionary of datasets
    """

    # define the output dictionary
    read_datasets = defaultdict(lambda: None)

    # define the filename re queries for each relevant dataset
    read_queries = {
        'consumers': RE_CONSUMERS,
        'consumer_user_mappings': RE_CONSUMER_USER_MAPPINGS,
        'transactions': RE_TRANSACTIONS,
        'merchants': RE_MERCHANTS
    }

    # define the reading functions for each relevant dataset
    read_functions = {
        'consumers': read_consumers,
        'consumer_user_mappings': read_consumer_user_mappings,
        'transactions': read_transactions,
        'merchants': read_merchants
    }

    # iterate through the filenames in the raw data path
    for filename in os.listdir(data_path):
        print(f'READING {data_path}/{filename}')
        rows_read = 0 # count the # of rows read for console output

        # iterate through each query and read dataset if it's relevant
        for table_name, query in read_queries.items():
            if re.search(query, filename):
                # read in the new data
                new_df = read_functions[table_name](spark, data_path, filename)

                # either append or create it
                read_datasets[table_name] = union_or_create(
                    read_datasets[table_name], new_df)

                # count # of rows read
                rows_read = new_df.count()

                # exit early since this dataset was read in correctly
                break

        print(f'\tL> {rows_read} ROWS READ')

    return read_datasets


def read_consumers(spark: SparkSession, data_path: str = DEFAULT_DATA_PATH,
        filename: str = 'tbl_consumer.csv') -> DataFrame:
    """ Read the consumer dataset.

    Args:
        spark (`SparkSession`): Spark session reading the data.
        data_path (str, optional): Path to all data. Defaults to '../data/tables'.
        filename (str, optional): The filename to read. Defaults to 'tbl_consumer.csv'.

    Returns:
        `DataFrame`: Resulting dataframe.
    """
    return spark.read.option(
            "delimiter", "|"
        ).csv(
            f'{data_path}/{filename}', 
            header = True,
        )

def read_consumer_user_mappings(spark: SparkSession, data_path: str = DEFAULT_DATA_PATH,
        filename: str = 'consumer_user_details.parquet') -> DataFrame:
    """ Read the `user_id` to `consumer_id` mapping dataset.

    Args:
        spark (`SparkSession`): Spark session reading the data.
        data_path (str, optional): Path to all data. Defaults to '../data/tables'.
        filename (str, optional): The filename to read. Defaults to 'consumer_user_details.parquet'.

    Returns:
        `DataFrame`: Resulting dataframe.
    """
    return spark.read.parquet(f'{data_path}/{filename}')

def read_transactions(spark: SparkSession, data_path: str = DEFAULT_DATA_PATH,
        folder: str = 'transactions_20210228_20210827_snapshot') -> DataFrame:
    """ Read the transaction dataset.

    Args:
        spark (`SparkSession`): Spark session reading the data.
        data_path (str, optional): Path to all data. Defaults to '../data/tables'.
        folder (str, optional): The folder to read. Defaults to 'transactions_20210228_20210827_snapshot'.
        
    Returns:
        `DataFrame`: Resulting dataframe.
    """

    # output df
    out_df = None

    # iterate over the subfolders in the transactions folder (ignore non-subfolder paths)
    for subfolder in os.listdir(f'{data_path}/{folder}'):
        if not os.path.isdir(f'{data_path}/{folder}/{subfolder}'): continue

        # read in this subfolder parquet
        temp_df = spark.read.parquet(f'{data_path}/{folder}/{subfolder}')

        # add the order_datetime as a column
        order_datetime = subfolder.split('=')[-1]
        temp_df = temp_df.withColumn('order_datetime', 
            F.lit(order_datetime))
            # F.lit(datetime.strptime(order_datetime, '%Y-%m-%d')))

        # add this to the output
        out_df = union_or_create(out_df, temp_df)

    return out_df

def read_merchants(spark: SparkSession, data_path: str = DEFAULT_DATA_PATH,
        filename: str = 'tbl_merchants.parquet') -> DataFrame:
    """ Read the merchant dataset.

    Args:
        spark (`SparkSession`): Spark session reading the data.
        data_path (str, optional): Path to all data. Defaults to '../data/tables'.
        filename (str, optional): The filename to read. Defaults to 'tbl_merchants.parquet'.
        
    Returns:
        `DataFrame`: Resulting dataframe.
    """
    return spark.read.parquet(f'{data_path}/{filename}')

