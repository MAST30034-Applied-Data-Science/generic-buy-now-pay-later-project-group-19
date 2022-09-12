''' Provides functions return summary information on the datasets.
'''

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def count_missing_values(df: DataFrame) -> DataFrame:
    """ Check missing values in each column of the spark dataframe.

    Args:
        df (`DataFrame`): Dataset to check.
    Returns:
        `DataFrame`: `DataFrame` with number of missing values in each column
    """
    return df.select(
            [
                F.count(
                    F.when(
                        F.col(c).contains('None') | \
                        F.col(c).contains('NULL') | \
                        (F.col(c) == '' ) | \
                        F.col(c).isNull() | \
                        F.isnan(c), 
                        c
                    )
                ).alias(c)
                for c, dtype in df.dtypes if dtype != 'date'
            ]
        )
