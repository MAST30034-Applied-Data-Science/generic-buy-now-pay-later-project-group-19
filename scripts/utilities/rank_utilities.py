''' Functions and utilities to rank data on different properties
TODO: Further commenting
'''
from h11 import Data
import numpy as np
import pandas as pd
from pandas import DataFrame

from utilities.log_utilities import logger

def add_column_rank(df: DataFrame, colname: str, ascending: bool = False,
    pct: bool = False) -> DataFrame:
    """ Add a new column for the rank of another column in the given df.
    This really exists so the column naming is kept standard.

    Args:
        df (`DataFrame`): Pandas `DataFrame` containing the data.
        colname (str): Name of the column to rank on.
        ascending (bool, optional): Whether to sort ascending (lower -> higher). 
            We typically want higher values ranked first.
            Defaults to False.
        pct (bool, optional): Whether to calculate a percentile value instead of an integer rank. 
            Defaults to False.
    Returns:
        `DataFrame`: The modified Pandas `DataFrame` containing the new rank column
    """
    rank_colname = f"{colname}_{'pctrank' if pct else 'rank'}"
    df[rank_colname] = df[colname].rank(
        method='average',
        ascending=ascending,
        pct=pct,
        na_option='bottom'
    )
    return df



def average_rank(df: DataFrame, colnames: 'list[str]', 
        pct: bool = False, weights: 'list[float]|None' = None) -> DataFrame:

    n = len(colnames)
    if weights == None:
        weights = np.ones((len(colnames), 1)) / n

    rank_colnames = [
        f"{cn}_{'pctrank' if pct else 'rank'}" for cn in colnames
    ]

    df['average_rank'] = np.matrix(df[rank_colnames]) @ weights

    return df