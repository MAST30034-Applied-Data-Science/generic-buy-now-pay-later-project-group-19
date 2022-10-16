''' Provides functions to read the datasets.
'''
from collections import defaultdict
import io
import os
import re
import functools
import requests
import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import pandas as pd

from utilities.log_utilities import logger
from utilities.write_utilities import DEFAULT_OUTPUT_DATA_PATH

DEFAULT_INPUT_DATA_PATH = './data/tables' # where the raw data is

# regex queries for finding the relevant datasets
RE_CONSUMERS = r'tbl_consumer.*\.csv'
RE_CONSUMER_USER_MAPPINGS = r'consumer_user_details.*\.parquet'
RE_TRANSACTIONS = r'transactions_\d+_\d+_snapshot'
RE_MERCHANTS = r'tbl_merchants.*\.parquet'
RE_CONSUMER_FRAUD = r'consumer_fraud_probability.*\.csv'
RE_MERCHANT_FRAUD = r'merchant_fraud_probability.*\.csv'

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

def read_data(spark: SparkSession, 
        data_path: str = DEFAULT_INPUT_DATA_PATH) -> 'defaultdict[str]':
    """ Read in all the relevant datasets placed in the one raw/tables folder.
    This makes assumptions about the naming schemes and file formats of each dataset,
    as per the `regex` queries defined at the top of this module.

    Args:
        spark (`SparkSession`): Spark session reading the data.
        data_path (str, optional): Path where the raw data is stored. 
            Defaults to `./data/path` (the relative location from script locations).

    Returns:
        defaultdict[str, `DataFrame` | None]: Output dictionary of datasets
    """

    # define the output dictionary
    data_dict = defaultdict(lambda: None)

    # define the filename re queries for each relevant dataset
    read_queries = {
        'consumers': RE_CONSUMERS,
        'consumer_user_mappings': RE_CONSUMER_USER_MAPPINGS,
        'transactions': RE_TRANSACTIONS,
        'merchants': RE_MERCHANTS,
        'consumer_fraud': RE_CONSUMER_FRAUD,
        'merchant_fraud': RE_MERCHANT_FRAUD
    }

    # define the reading functions for each relevant dataset
    read_functions = {
        'consumers': read_consumers,
        'consumer_user_mappings': read_consumer_user_mappings,
        'transactions': read_transactions,
        'merchants': read_merchants,
        'consumer_fraud': read_consumer_fraud,
        'merchant_fraud': read_merchant_fraud
    }

    # iterate through the filenames in the raw data path
    for filename in os.listdir(data_path):
        # iterate through each query and read dataset if it's relevant
        for table_name, query in read_queries.items():
            if re.search(query, filename):
                logger.debug(f'READING {data_path}/{filename}')

                # read in the new data
                new_df = read_functions[table_name](spark, data_path, filename)

                # either append or create it
                data_dict[table_name] = union_or_create(
                    data_dict[table_name], new_df)

                # count # of rows read
                logger.info(f'{new_df.count()} ROWS READ FROM {data_path}/{filename}')

                # exit early since this dataset was read in correctly
                break

    # read in the external datasets
    data_dict['postcodes'] = read_postcodes(spark)
    logger.info(f'{data_dict["postcodes"].count()} ROWS READ FOR POSTCODES DATA')
    data_dict['census'] = read_census(spark)
    logger.info(f'{data_dict["census"].count()} ROWS READ FOR CENSUS DATA')

    return data_dict

def read_consumers(spark: SparkSession, data_path: str = DEFAULT_INPUT_DATA_PATH,
        filename: str = 'tbl_consumer.csv') -> DataFrame:
    """ Read the consumer dataset.

    Args:
        spark (`SparkSession`): Spark session reading the data.
        data_path (str, optional): Path to all data. Defaults to './data/tables'.
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

def read_consumer_user_mappings(spark: SparkSession, 
        data_path: str = DEFAULT_INPUT_DATA_PATH, 
        filename: str = 'consumer_user_details.parquet') -> DataFrame:
    """ Read the `user_id` to `consumer_id` mapping dataset.

    Args:
        spark (`SparkSession`): Spark session reading the data.
        data_path (str, optional): Path to all data. Defaults to './data/tables'.
        filename (str, optional): The filename to read. Defaults to 'consumer_user_details.parquet'.

    Returns:
        `DataFrame`: Resulting dataframe.
    """
    return spark.read.parquet(f'{data_path}/{filename}')

def read_all_transactions(spark: SparkSession, 
        data_path: str = DEFAULT_INPUT_DATA_PATH) -> DataFrame:
    """ Reading only all the transactions dataframes in one go

    Args:
        spark (`SparkSession`): Spark session reading the data.
        data_path (str, optional): Path to all data. Defaults to './data/tables'.

    Returns:
        `DataFrame`: Resulting dataframe.
    """

    # init an empty df
    output_df = None

    # iterate through the filenames in the raw data path
    for filename in os.listdir(data_path):
        if re.search(RE_TRANSACTIONS, filename):
            logger.debug(f'READING {data_path}/{filename}')

            # read in the new data
            new_df = read_transactions(spark, data_path, filename)

            # either append or create it
            output_df = union_or_create(output_df, new_df)

    return output_df

def read_transactions(spark: SparkSession, data_path: str = DEFAULT_INPUT_DATA_PATH,
        folder: str = 'transactions_20210228_20210827_snapshot') -> DataFrame:
    """ Read the transaction dataset.

    Args:
        spark (`SparkSession`): Spark session reading the data.
        data_path (str, optional): Path to all data. Defaults to './data/tables'.
        folder (str, optional): The folder to read. Defaults to 'transactions_20210228_20210827_snapshot'.
        
    Returns:
        `DataFrame`: Resulting dataframe.
    """

    # output df
    out_df = None

    try: # this should work for 99% of people

        logger.debug('Trying to read transactions the nice way.')
        out_df = spark.read.parquet(f'{data_path}/{folder}').withColumn(
            'order_datetime', 
            F.date_format('order_datetime', "yyyy-MM-dd")
        )
        
    except: # specifically for and by Tommy (the rest of the group shouldn't activate this code)

        logger.error('''Something went wrong with reading transactions,''' 
        + ''' so I'm using Tommy's method. If you're not Tommy,'''
        + ''' something may have gone wrong.''')

        # get the files in the folder
        list_files = os.listdir(f'{data_path}/{folder}')
        list_files = list_files[1:(len(list_files)-1)]

        # explicit function
        def union_all(dfs) -> DataFrame:
            """ Stack all the `DataFrames`.

            Args:
                dfs: the datasets to merge vertically.

            Returns:
                `DataFrame`: Output/stacked dataset
            """
            return functools.reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)

        # read files
        file_names = os.listdir(f'{data_path}/{folder}/' + list_files[0])
        
        # just keep the `.parquet` file only
        def get_parquet_filename(file_names: list(str)) -> str:
            """ Get the filename of the only `.parquet` file in the
             `order_datetime` folder.

            Args:
                file_names (list[str]): List of filenames to check

            Returns:
                str: The filename of the only/first `.parquet` file.
            """
            for fn in file_names:
                if fn.split('.')[-1] == '.parquet':
                    return fn

        file_name = get_parquet_filename(file_names)

        # start the output unioned df
        out_df = spark.read.parquet(f'{data_path}/{folder}/' + list_files[0] +"/" + file_name)
        out_df = out_df.withColumn('order_datetime',F.lit(list_files[0].split('=')[-1]))
        
        # iterate through the rest of the parquet files and union them
        for i in list_files[1:]:

            # get the filename
            file_names = os.listdir(f'{data_path}/{folder}/' + i)
            file_name = get_parquet_filename(file_names)

            # extract this folder's data
            tmp = spark.read.parquet(f'{data_path}/{folder}/' + i + "/" + file_name)
            tmp = tmp.withColumn('order_datetime', F.lit(i[15:]))

            # merge
            out_df = union_all([out_df, tmp] )

    return out_df

def read_merchants(spark: SparkSession, data_path: str = DEFAULT_INPUT_DATA_PATH,
        filename: str = 'tbl_merchants.parquet') -> DataFrame:
    """ Read the merchant dataset.

    Args:
        spark (`SparkSession`): Spark session reading the data.
        data_path (str, optional): Path to all data. Defaults to './data/tables'.
        filename (str, optional): The filename to read. Defaults to 'tbl_merchants.parquet'.
        
    Returns:
        `DataFrame`: Resulting dataframe.
    """
    return spark.read.parquet(f'{data_path}/{filename}')

def read_postcodes(spark: SparkSession):
    """ Download and read the postcode data.

    Args:
        spark (`SparkSession`): Spark session reading the data.

    Returns:
        `DataFrame`: Resulting dataframe.
    """
    url = "https://www.matthewproctor.com/Content/postcodes/australian_postcodes.csv"
    req_data = requests.get(url).content
    postcode_df = pd.read_csv(io.StringIO(req_data.decode('utf-8')))
    postcode_df = postcode_df[['postcode', 'SA2_MAINCODE_2016']]
    return spark.createDataFrame(postcode_df)\
        .withColumnRenamed('SA2_MAINCODE_2016', 'sa2_code')

def read_census(spark: SparkSession, data_path: str = DEFAULT_INPUT_DATA_PATH,
        filename: str = 'SA2/AUS/2021Census_G02_AUST_SA2.csv'):
    """ Read the external SA2 Census (2021) dataset.

    Args:
        spark (`SparkSession`): Spark session reading the data.
        data_path (str, optional): Path to all data. Defaults to './data/tables'.
        filename (str, optional): The filename to read. Defaults to 'SA2/AUS/2021Census_G02_AUST_SA2.csv'.

    Returns:
        `DataFrame`: Resulting dataframe.
    """
    # read the census data
    census_df = spark.read.csv(f'{data_path}/{filename}', header = True)
    census_df = census_df.select([
        F.col(colname).alias(colname.lower()) for colname in census_df.columns
    ])

    # get the column for the sa2 codes
    sa2_code_colname = ''
    for colname in census_df.columns:
        if re.search(r'sa2_code_\d{4}', colname.lower()) is not None:
            logger.debug(f'The SA2 colname is "{colname}"')
            sa2_code_colname = colname.lower()
            
    # conver the sa2 codes to integer types
    census_df = census_df.withColumn(
        sa2_code_colname, 
        census_df[sa2_code_colname].cast(IntegerType())
    )

    # make the column names all lowercase and rename the sa2 code column
    return census_df.select([
        F.col(colname).alias(colname.lower()) for colname in census_df.columns
    ]).withColumnRenamed(sa2_code_colname, 'sa2_code')

def read_consumer_fraud(spark: SparkSession, 
        data_path: str = DEFAULT_INPUT_DATA_PATH,
        filename: str = 'consumer_fraud_probability.csv'):
    """ Read consumer fraud data.

    Args:
        spark (`SparkSession`): Spark session reading the data.
        data_path (str, optional): Path to all data. Defaults to './data/tables'.
        filename (str, optional): The filename to read. Defaults to 'consumer_fraud_probability.csv'.

    Returns:
        _type_: _description_
    """

    return spark.read.csv(
            f'{data_path}/{filename}', 
            header = True,
        ).withColumn(
            'fraud_probability', 
            F.col('fraud_probability').cast('float')
        )

def read_merchant_fraud(spark: SparkSession, 
        data_path: str = DEFAULT_INPUT_DATA_PATH,
        filename: str = 'merchant_fraud_probability.csv'):
    """ Read merchant fraud data.

    Args:
        spark (`SparkSession`): Spark session reading the data.
        data_path (str, optional): Path to all data. Defaults to './data/tables'.
        filename (str, optional): The filename to read. Defaults to 'merchant_fraud_probability.csv'.

    Returns:
        _type_: _description_
    """

    return spark.read.csv(
            f'{data_path}/{filename}', 
            header = True,
        ).withColumn(
            'fraud_probability', 
            F.col('fraud_probability').cast('float')
        )

    """ Read the merchant statistics data in Pandas

    Args:
        data_path (str, optional): _description_. Defaults to DEFAULT_INPUT_DATA_PATH.
    """

def pd_read_merchant_statistics(
        data_path: str = DEFAULT_OUTPUT_DATA_PATH) -> pd.DataFrame:
    """ Read the merchant statistics data in Pandas

    Args:
        data_path (str, optional): Path of the curated data. Defaults to `./data/curated`.

    Returns:
        `pd.DataFrame`: Output merchant statistics.
    """

    return pd.read_parquet(f'{data_path}/final_merchant_statistics')
