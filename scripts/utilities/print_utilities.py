''' Functions for printing in the console in a nice way.
'''

from collections import defaultdict
import logging
import pprint

from pyspark.sql import DataFrame as SDF
from pandas import DataFrame as PDF
from pandas import Series as PS
from geopandas import GeoDataFrame as GDF

from utilities.log_utilities import logger
import utilities.info_utilities as INFO

def str_df_head(df, rows: int = 20, cols: int = 10) -> str:
    """ Generates the output string for the head of any type of `DataFrame`.

    Args:
        df (`AnyDataFrame`): The `DataFrame`. Can be 
            - `Pandas`, 
            - `PySpark`, 
            - `GeoPandas`,
            - or list.
        rows (int, optional): Number of rows to print. Defaults to 20.
        cols (int, optional): Number of columns to print. Defaults to 10.

    Returns:
        str: Output string.
    """

    if type(df) == SDF: # Spark DataFrame
        df:SDF = df
        return df.select(df.columns[:min(cols, len(df.columns))]).head(rows)
    
    elif type(df) == PDF or type(df) == PS: # Pandas Series or DataFrame
        if type(df) == PS: # specifically Pandas Series
            df:PS = df
            return df.to_string(max_rows=rows)
        
        df:PDF = df
        return df.to_string(max_rows=rows, max_cols=cols)

    elif type(df) == GDF: # Geopandas DataFrame
        df:GDF = df
        return df.head(rows)
    
    elif type(df) == list: # Just a list lol
        return df[:rows]

    logger.error(f'This is not a known DataFrame type {type(df)}')
    return 'This is not a known DataFrame type.'

def print_script_header(header: str):
    """ Prints the header for a script's code block in the console output.

    Args:
        header (str): Header title
    """
    logger.info(
        f'''
    === {header.upper()}
    {50 * '='}
        '''
    )

def print_dataset_summary(data_dict: 'defaultdict[str]',
        datasets: 'list[str]|None' = None):
    """ Prints summary information on all, or specific dataset(s) within the 
    given dataset dictionary.

    Args:
        data_dict (defaultdict[str, `DataFrame` | None]): input dataset dictionary
        datasets (list[str] | None): the key(s) of the specific dataset(s) to show (`None` to show all)
    """

    # iterate through the datasets
    for dataset_name, df in data_dict.items():
        # skip datasets which arent in the list (assume None means use all datasets)
        if datasets is not None and dataset_name not in datasets: continue
        
        logger.info(f'Summary of {dataset_name}')
        if type(df) == SDF: # Spark DataFrame
            df:SDF = df
            logger.info(pprint.pformat(df.schema))

        if logger.level == logging.DEBUG:  # print the rows if debugging
            logger.debug(f'Printing first 20 rows from {dataset_name}')
            logger.debug(f'{str_df_head(df)}')

        if type(df) == SDF: # Spark DataFrame
            if logger.level == logging.DEBUG:
                logger.debug(f'Check missing values in the {dataset_name} dataset')
                logger.debug(f'\n{INFO.count_missing_values(df)}')

def capitalized_spaced(colname: str) -> str:
    """ Take the colname, separate, replacing `_` with ` ` and capitalize the words.

    Args:
        colname (str): Name of the column

    Returns:
        str: The Prettified, capitalized and spaced column name.
    """
    return ' '.join([
        w.capitalize()
        for w in colname.split('_')
    ])