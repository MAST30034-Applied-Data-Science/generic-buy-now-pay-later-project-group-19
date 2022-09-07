''' Provides functions to read the datasets.
'''

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
import re
import numpy as np

def check_missing_values(sdf: DataFrame) -> DataFrame:
    """ Check missing values in each column of the spark dataframe
    Args:
        sdf: Spark DataFrame
    Returns:
        `DataFrame`: DataFrame detailing number of missing values in each column
    """
    return sdf.select(
            [count(when(
                col(c).contains('None') | \
                col(c).contains('NULL') | \
                (col(c) == '' ) | \
                col(c).isNull() | \
                isnan(c), c)
             ).alias(c)
            for c, dtype in sdf.dtypes if dtype != 'date']
        )


def extract_tags(merchant_sdf: DataFrame) -> DataFrame:
    """ Extract tags, revenue level and take rate from tags column in the 
    merchant dataset
    Args:
        merchant_sdf (`DataFrame`): merchant spark dataframe
    Returns:
        `DataFrame`: Resulting DataFrame
    """
    
    # Since merchant df is small, we will use pandas for convenience
    merchant_sdf = merchant_sdf.toPandas()
    
    def extract(arr: str, category: str = 'tags'):
        """ Extract tags, revenue level and take rate from tags
        given in the form '((tags), (revenue_level), (take_rate))'
        Args:
            arr (str): array in the tag column
            category (str): 'tags', 'revenue_level' or 'take_rate'
        Returns:
            category_value (str or float): value extracted by the category
        """

        # Split tags into the three components
        arr = arr[1:-1]
        split_arr = re.split('\), \(|\], \[', arr.strip('[()]'))

        if category == 'take_rate':
            return float(re.findall('[\d\.\d]+', split_arr[2])[0])

        elif category == 'revenue_level':
            return split_arr[1].lower()

        # by default return tags
        return split_arr[0].lower()
    
    
    # Split information into separate columns
    for col in ['tag', 'revenue_level', 'take_rate']:
        merchant_sdf[col] = merchant_sdf['tags'].apply(
            lambda x: extract(x, col)
        )

    return merchant_sdf.drop(
            'tags',
            axis=1
        )


def remove_transaction_outliers(transaction_sdf: DataFrame) -> DataFrame:
    """ Check outliers from the transaction dataset
    Args:
        transaction_sdf: transaction Spark DataFrame
    Returns:
        `DataFrame`: DataFrame post outlier removal
    """
    
    transaction_sdf = transaction_sdf.withColumn(
        'log(dollar_value)',
        log(col('dollar_value'))
    )

    # Find lower and upper bound
    lwr, upr = transaction_sdf.approxQuantile('log(dollar_value)', [0.25, 0.75], 0)
    iqr = upr - lwr
    lwr_bound = lwr - 1.5 * iqr
    upr_bound = upr + 1.5 * iqr
    
    lwr_bound, upr_bound = np.exp(lwr_bound), np.exp(upr_bound)
    
    # Filter data by lower and upper bound
    new_transaction = transaction_sdf.where(
        (col('dollar_value') >= lwr_bound) &
        (col('dollar_value') <= upr_bound)
    )
    
    # Summary of outlier removal
    print(f"Outlier Removal Summary:")
    pre_removal = transaction_sdf.count()
    post_removal = new_transaction.count()
    print(f"New range of dollar per transaction: {lwr_bound:.2f} - {upr_bound:.2f}",
          f"\nNumber of instances after outlier removal: {post_removal}",
          f"\nNumber of outliers removed: {pre_removal - post_removal}",
          f"\n% data removed: {((pre_removal - post_removal)/pre_removal)*100:.2f}%")
    
    return new_transaction


