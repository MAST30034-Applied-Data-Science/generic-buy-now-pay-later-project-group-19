''' Functions and utilities to rank data on different properties
'''
import numpy as np
import pandas as pd
from pandas import DataFrame

from utilities.log_utilities import logger

def add_column_rank(df: DataFrame, colname: str, ascending: bool = False,
    rank_type: str = 'minmax') -> DataFrame:
    """ Add a new column for the rank of another column in the given df.
    This really exists so the column naming is kept standard.

    Args:
        df (`DataFrame`): Pandas `DataFrame` containing the data.
        colname (str): Name of the column to rank on.
        ascending (bool, optional): Whether to sort ascending (lower -> higher). 
            We typically want higher values ranked first.
            Defaults to False.
        rank_type (str): The type of ranking to perform. Defaults to 'minimax'.
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


def min_max_scale(column: 'Iterable') -> 'Iterable':
    """ Perform min-max scaling on a column

    Args:
        column (`Iterable`): the list/column to perform scaling on

    Returns:
        `Iterable`: The min-max-scaled list
    """
    
    if np.max(column) == np.min(column) : 
        return np.ones((len(column), 1)) / 2
    else : 
        return (column - np.min(column)) / (np.max(column) - np.min(column))


def average_rank(df: DataFrame, colnames: 'list[str]', 
        rank_type: str = 'minmax', weights: 'list[float]|None' = None,
        suffix: str = '') -> DataFrame:
    """ Generate the weighted/unweighted average rank based off of ranking columns.

    Args:
        df (`DataFrame`): Dataset to rank.
        colnames (list[str]): Name of columns to rank over.
        rank_type (str, optional): Type of ranking to perform/look for. Defaults to 'minmax'.
        weights (list[float]|None, optional): weights of the columns. Defaults to None.
        suffix (str): An optional suffix to add to the new columns.

    Returns:
        `DataFrame`: Ranked dataset.
    """

    n = len(colnames)
    if weights == None:
        weights = np.ones((1, len(colnames))) / n

    # get the column names to rank over
    rank_colnames = [
        f'{rank_type}_{cn}' for cn in colnames
    ]

    if len(suffix) > 0: suffix = '_' + suffix

    # get the linear combination of the rank columns
    df[f'average_{rank_type}{suffix}'] = np.array(np.matrix(df[rank_colnames]) @ np.matrix(weights).T)

    return df


def bias_weights(all_columns: 'list[str]', bias_columns: 'list[str]', 
    bias_multiplier: float = 2) -> 'list[str]':
    """ Generate weights for ranking with a bias towards a certain set of columns.

    Args:
        all_columns (list[str]): The ordered list of columns to rank on.
        bias_columns (list[str]): The columns for which to multiply the weight.
        bias_multiplier (float, optional): How much to multiply the weights by. 
            Defaults to 2.

    Returns:
        list[str]: Ordered list of weights for the columns with bias applied.
    """

    # determine current even weight
    current_weight = 1 / len(all_columns)

    # apply bias multiplier
    bias_weight = bias_multiplier * current_weight


    # calculate the new total biased weights
    total_bias_weight = len(bias_columns) * bias_weight

    if(total_bias_weight >= 1):
        logger.error('Hey, that\'s too much of bias multiplier')
        return None

    # calculate the weights of the remaining values (should sum to 1)
    new_remaining_weight = (1 - total_bias_weight)
    new_remaining_weight /= (len(all_columns) - len(bias_columns))

    # generate the list of weights
    new_weights = []
    for colname in all_columns:
        if colname in bias_columns:
            new_weights.append(bias_weight)
        else: 
            new_weights.append(new_remaining_weight)

    return new_weights