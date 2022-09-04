''' Provides functions to read the datasets.
'''

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
import re

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
    



