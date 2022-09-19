''' Provide functionality to aggregate datasets.
TODO: commenting on this
'''
from collections import defaultdict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
import pandas as pd
from datetime import datetime

# The list of any aggregation functions that I could possible need
AGGREGATION_FUNCTIONS = {
    'total': ('tot_', F.sum),
    'average': ('avg_', F.avg),
    'count': ('num_', F.count),
}

def compute_aggregates(spark: SparkSession, data_dict: 'defaultdict[str]'):
    
    data_dict['merchant_sales'] = compute_merchant_sales(spark,
        data_dict['transactions'], data_dict['merchants'])
    data_dict['customer_accounts'] = compute_customer_accounts(spark,
        data_dict['consumers'], data_dict['consumer_user_mappings'])
    data_dict['merchant_summary'] = compute_merchant_metric(spark, 
        data_dict['merchant_sales'], data_dict['merchants'])
    
    return data_dict

def compute_merchant_sales(spark: SparkSession, transaction_df: DataFrame, 
        merchant_df: DataFrame) -> 'defaultdict[str]':
    # TODO:Commenting here

    return transaction_df \
        .groupby('merchant_abn', 'order_datetime') \
        .agg({'dollar_value':'sum', 'order_id':'count'}) \
        .withColumnRenamed('sum(dollar_value)', 'sales_revenue') \
        .withColumnRenamed('count(order_id)', 'no_orders')
    

def compute_customer_accounts(spark: SparkSession, consumer_df: DataFrame, 
        consumer_user_mapping_df: DataFrame) -> DataFrame:
    
    return consumer_df.join(
        consumer_user_mapping_df,
        on = 'consumer_id'
    )

def compute_customer_transactions(spark: SparkSession, 
        transactions_df: DataFrame,
        customer_accounts_df: DataFrame) -> DataFrame:
    
    return transactions_df.join(
        customer_accounts_df,
        on='user_id'
    )

def compute_merchant_metric(spark: SparkSession, merchant_sales: DataFrame,
                           merchant: DataFrame) -> DataFrame:
    
    
    # This part is taking a while 
    date_range = merchant_sales.select(F.min(F.col("order_datetime")), 
                                       F.max(F.col("order_datetime"))
                                      ).first()
    
    min_date, max_date = (datetime.strptime(date_range[0], "%Y-%m-%d"), 
                          datetime.strptime(date_range[1], "%Y-%m-%d"))
    
    num_days = (max_date - min_date).days
    
    # Group first to reduce the table size before joining
    merchant_daily_sales = merchant_sales.groupby('merchant_abn').agg(
        (F.sum(F.col('sales_revenue')) / num_days).alias('avg_daily_rev'),
        (F.sum(F.col('sales_revenue')) / F.sum(F.col('no_orders'))).alias('avg_value_per_order'),
        (F.sum(F.col('no_orders')) / num_days).alias('avg_daily_order')
    )
    
    merchant_daily_sales = merchant.join(
        merchant_daily_sales, 
        on=["merchant_abn"],
        how='left'
    ).toPandas()
    
    
    merchant_daily_sales['avg_daily_commission'] = (merchant_daily_sales['avg_daily_rev'] *
                                                    (merchant_daily_sales['take_rate']/100))
    
    merchant_daily_sales['avg_commission_per_order'] = (merchant_daily_sales['avg_value_per_order'] *
                                                        (merchant_daily_sales['take_rate']/100))
    
    
    return merchant_daily_sales

# def compute_sales_by_region():
#     print('')

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