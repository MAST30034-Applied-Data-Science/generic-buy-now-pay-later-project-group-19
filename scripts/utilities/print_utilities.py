''' Functions for printing in the console in a nice way.
'''

from collections import defaultdict

from pyspark.sql import DataFrame as SDF
from pandas import DataFrame as PDF
from pandas import Series as PS
from geopandas import GeoDataFrame as GDF

from utilities.log_utilities import logger
import utilities.info_utilities as INFO

def str_df_head(df, n: int = 20) -> str:
    """ Generates the output string for the head of any type of `DataFrame`.

    Args:
        df (`pyspark.sql.DataFrame` | `pandas.DataFrame` | `geopandas.GeoDataFrame`): Input
        n (int, optional): Number of rows to return. Defaults to 20.

    Returns:
        str: the string containing the "head".
    """
    if type(df) == SDF:
        return df.show(n)
    elif type(df) == PDF or type(df) == PS:
        return df.to_string(max_rows=n)
    elif type(df) == GDF:
        return df.head(n)
    elif type(df) == list:
        return df[:n]
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
    for dataset_name, df in data_dict.items():
        if datasets is not None and dataset_name not in datasets: continue
        logger.debug(f'Printing first 20 rows from {dataset_name}')
        logger.debug(f'{str_df_head(df)}')
        if type(df) == SDF:
            logger.info(f'Check missing values in the {dataset_name} dataset')
            logger.info(f'\n{INFO.count_missing_values(df)}')