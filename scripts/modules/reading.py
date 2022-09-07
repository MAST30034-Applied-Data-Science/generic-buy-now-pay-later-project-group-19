''' Provides functions to read the datasets.
'''

import os
import re
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DEFAULT_DATA_PATH = '../data/tables'
RE_CONSUMERS = r'tbl_consumer.csv'
RE_CONSUMER_USER_MAPPINGS = r'consumer_user_details.parquet'
RE_TRANSACTIONS = r'transactions_\d+_\d+_snapshot'
RE_MERCHANTS = r'tbl_merchants.parquet'

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

def read_data(spark: SparkSession, data_path: str = DEFAULT_DATA_PATH) -> DataFrame:

    # consumer_df = None
    # consumer_user_mappings_df = None
    # transactions_df = None
    # merchants_df = None

    read_datasets = {
        'consumers': None,
        'consumer_user_mappings': None,
        'transactions': None,
        'merchants': None
    }

    read_queries = {
        'consumers': RE_CONSUMERS,
        'consumer_user_mappings': RE_CONSUMER_USER_MAPPINGS,
        'transactions': RE_TRANSACTIONS,
        'merchants': RE_MERCHANTS
    }

    read_functions = {
        'consumers': read_consumers,
        'consumer_user_mappings': read_consumer_user_mappings,
        'transactions': read_transactions,
        'merchants': read_merchants
    }

    for filename in os.listdir(data_path):
        print(f'READING {data_path}/{filename}')
        rows_read = 0

        for table_name, query in read_queries.items():
            if re.search(query, filename):
                new_df = read_functions[table_name](spark, data_path, filename)
                read_datasets[table_name] = union_or_create(
                    read_datasets[table_name], new_df)
                rows_read = new_df.count()
                break

        # if re.search(RE_CONSUMERS, filename):
            
        #     new_consumer_df = read_consumers(spark, data_path, filename)
        #     consumer_df = union_or_create(consumer_df, new_consumer_df)
        #     rows_read = new_consumer_df.count()
        # elif re.search(RE_CONSUMER_USER_MAPPINGS, filename):
        #     consumer_user_mappings_df = union_or_create(
        #         consumer_user_mappings_df, 
        #         read_consumer_user_mappings(
        #             spark, data_path, filename
        #         ))
        # elif re.search(RE_TRANSACTIONS, filename):
        #     transactions_df = union_or_create(
        #         transactions_df, 
        #         read_transactions(spark, data_path, filename))
        # elif re.search(RE_MERCHANTS, filename):
        #     merchants_df = union_or_create(
        #         merchants_df,
        #         read_merchants(spark, data_path, filename))
        print(f'\tL> {rows_read} ROWS READ')

    return read_datasets
    # return {
    #     'consumers': consumer_df, 
    #     'users': consumer_user_mappings_df,
    #     'transactions': transactions_df,
    #     'merchants': merchants_df
    # }


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
        folder: str = 'transactions_20210228_20210827_snapshot/') -> DataFrame:
    """ Read the transaction dataset.

    Args:
        spark (`SparkSession`): Spark session reading the data.
        data_path (str, optional): Path to all data. Defaults to '../data/tables'.
        filename (str, optional): The filename to read. Defaults to 'transactions_20210228_20210827_snapshot/'.
        
    Returns:
        `DataFrame`: Resulting dataframe.
    """

    out_df = None

    for subfolder in os.listdir(f'{data_path}/{folder}'):
        if not os.path.isdir(f'{data_path}/{folder}/{subfolder}'): continue

        temp_df = spark.read.parquet(f'{data_path}/{folder}/{subfolder}')

        order_datetime = subfolder.split('=')[-1]
        temp_df = temp_df.withColumn('order_datetime', 
            F.lit(datetime.strptime(order_datetime, '%Y-%m-%d')))

        out_df = union_or_create(out_df, temp_df)

    return out_df

    # return spark.read.format("parquet").load(f'{data_path}/{filename}')
    # return spark.read.parquet(f'{data_path}/{filename}')

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

