''' Functions and utilities to rank data on different properties
TODO: Further commenting
'''
import numpy as np
import pandas as pd
from pandas import DataFrame

from utilities.log_utilities import logger

def add_column_rank(df: DataFrame, colname: str, ascending: bool = False,
    rank_type: str = 'rank') -> DataFrame:
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
    rank_colname = f'{rank_type}_{colname}'
    if rank_type == 'minmax':
        df[rank_colname] = min_max_scale(df[colname])
        if not ascending:
            df[rank_colname] = 1 - df[rank_colname]
    else:
        pct = True if rank_type == 'pct' else False
        df[rank_colname] = df[colname].rank(
            method='average',
            ascending=ascending,
            pct=pct,
            na_option='bottom'
        )
    return df

# min_max_scale
def min_max_scale(column):
    #data = [i for i in data if np.isnan(i) == False]
    if np.max(column) == np.min(column) : return np.ones((len(column), 1)) / 2
    else : return (column - np.min(column)) / (np.max(column) - np.min(column))

def average_rank(df: DataFrame, colnames: 'list[str]', 
        rank_type: str = 'rank', weights: 'list[float]|None' = None,
        suffix: str = '') -> DataFrame:

    n = len(colnames)
    if weights == None:
        weights = np.ones((1, len(colnames))) / n

    rank_colnames = [
        f'{rank_type}_{cn}' for cn in colnames
    ]

    if len(suffix) > 0: suffix = '_' + suffix

    # print(np.matrix(df[rank_colnames]).shape)
    # print(np.matrix(weights).T.shape)

    df[f'average_rank{suffix}'] = np.matrix(df[rank_colnames]) @ np.matrix(weights).T

    return df