''' Provides functions to read the datasets.
'''

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

def read_consumers(spark: SparkSession, data_path: str = '../data/tables',
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

def read_consumer_user_mappings(spark: SparkSession, data_path: str = '../data/tables',
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

def read_transactions(spark: SparkSession, data_path: str = '../data/tables',
        filename: str = 'transactions_20210828_20220227_snapshot/') -> DataFrame:
    """ Read the transaction dataset.

    Args:
        spark (`SparkSession`): Spark session reading the data.
        data_path (str, optional): Path to all data. Defaults to '../data/tables'.
        filename (str, optional): The filename to read. Defaults to 'transactions_20210228_20210827_snapshot/'.
        
    Returns:
        `DataFrame`: Resulting dataframe.
    """
    print(filename)
    return spark.read.parquet(f'{data_path}/{filename}')

def read_merchants(spark: SparkSession, data_path: str = '../data/tables',
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

