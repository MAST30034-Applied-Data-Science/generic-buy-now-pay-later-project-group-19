''' Provide functionality to aggregate datasets.
'''
from collections import defaultdict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
import pandas as pd

# The list of any aggregation functions that I could possible need
AGGREGATION_FUNCTIONS = {
    'total': ('tot_', F.sum),
    'average': ('avg_', F.avg),
    'count': ('num_', F.count),
}

def compute_aggregates():
    print('')

def compute_merchant_sales(spark: SparkSession, transactions_df: DataFrame, merchants_df: pd.DataFrame):
    merchant_sales_df = transactions_df \
        .groupby('merchant_abn', 'order_datetime') \
        .agg({'dollar_value':'sum', 'order_id':'count'}) \
        .withColumnRenamed('sum(dollar_value)', 'sales_revenue') \
        .withColumnRenamed('count(order_id)', 'no_orders')
    
    merchant_sales_df = merchant_sales_df.toPandas().join(
        merchants_df,
        on='merchant_abn',
        how='right'
    )

    return merchant_sales_df

def compute_customer_spending():
    print('')

def compute_sales_by_region():
    print('')

def group_and_aggregate(df: DataFrame, group_cols: "list[str]", agg_cols: dict) -> DataFrame:
    """ Group a dataset and aggregate it using the chosen functions

    Args:
        df (`DataFrame`): The dataset to aggregate. 
        pop_df (`DataFrame`): The dataframe of borough populations per year.
        group_cols (list[str]): The columns to group the dataset by.
        agg_cols (dict): The aggregation functions used.

    Returns:
        `DataFrame`: The aggregated dataframe
    """

    # group using the defined columns
    grouped_df = df.groupBy(group_cols)
    
    # define the list of aggregated columns to add
    column_aggregates = []

    # iterate through the defined aggregations,
    # perform the aggregation and name the columsn accordingly
    for colname, func_types in agg_cols.items():
        for func_type in func_types:
            prefix, func = AGGREGATION_FUNCTIONS[func_type]

            column_aggregates.append(
                func(colname).alias(f'{prefix}{colname}')
            )

    # return the grouped data with aggregations
    return grouped_df.agg(*column_aggregates)