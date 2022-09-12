''' Provides functions to clean the datasets.
'''

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import re
import numpy as np

from utilities.log_utilities import logger

def extract_merchant_tags(merchant_df: DataFrame) -> DataFrame:
    """ Extract tags, revenue level and take rate from tags column in the 
    merchant dataset

    Args:
        merchant_df (`DataFrame`): merchant spark dataframe
    Returns:
        `DataFrame`: Resulting `DataFrame`
    """
    
    # Since merchant df is small, we will use pandas for convenience
    merchant_df = merchant_df.toPandas()
    
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
        merchant_df[col] = merchant_df['tags'].apply(
            lambda x: extract(x, col)
        )

    return merchant_df.drop(
            'tags',
            axis=1
        )


def remove_transaction_outliers(transaction_df: DataFrame) -> DataFrame:
    """ Check outliers from the transaction dataset
    Args:
        transaction_df: transaction Spark DataFrame
    Returns:
        `DataFrame`: Resulting `DataFrame`
    """
    
    transaction_df = transaction_df.withColumn(
        'log(dollar_value)',
        F.log(F.col('dollar_value'))
    )

    # Find lower and upper bound
    lwr, upr = transaction_df.approxQuantile('log(dollar_value)', [0.25, 0.75], 0)
    iqr = upr - lwr
    lwr_bound = lwr - 1.5 * iqr
    upr_bound = upr + 1.5 * iqr
    
    lwr_bound, upr_bound = np.exp(lwr_bound), np.exp(upr_bound)
    
    # Filter data by lower and upper bound
    new_transaction = transaction_df.where(
        (F.col('dollar_value') >= lwr_bound) &
        (F.col('dollar_value') <= upr_bound)
    )
    
    # Summary of outlier removal
    logger.info(f"Outlier Removal Summary:")
    pre_removal = transaction_df.count()
    post_removal = new_transaction.count()
    logger.info(f"New range of dollar per transaction: {lwr_bound:.2f} - {upr_bound:.2f}")
    logger.info(f"\nNumber of instances after outlier removal: {post_removal}")
    logger.info(f"\nNumber of outliers removed: {pre_removal - post_removal}")
    logger.info(f"\n% data removed: {((pre_removal - post_removal)/pre_removal)*100:.2f}%")
    
    return new_transaction

