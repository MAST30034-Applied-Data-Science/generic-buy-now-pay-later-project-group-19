''' Provide functionality to aggregate datasets.
TODO: commenting on this
'''
from collections import defaultdict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
import pandas as pd
from datetime import datetime
from pyspark.sql.types import IntegerType

# The list of any aggregation functions that I could possible need
AGGREGATION_FUNCTIONS = {
    'total': ('tot_', F.sum),
    'average': ('avg_', F.avg),
    'count': ('num_', F.count),
}

def compute_aggregates(spark: SparkSession, data_dict: 'defaultdict[str]'):
    
    # Helper aggregates
    data_dict['merchant_sales'] = compute_merchant_sales(spark,
        data_dict['transactions'], data_dict['merchants'])
    
    data_dict['customer_accounts'] = compute_customer_accounts(spark,
        data_dict['consumers'], data_dict['consumer_user_mappings'])
    
    data_dict['merchant_consumer'] = compute_merchant_consumer(spark,
        data_dict['transactions'])
    
    data_dict['consumer_region'] = compute_consumer_region(spark, 
        data_dict['consumers'], data_dict['postcodes'], data_dict['consumer_user_mappings'])
    
    data_dict['consumer_region_income'] = compute_region_income(spark, 
        data_dict['consumer_region'], data_dict['census'])
    
    # Metrics aggregates to be joined (the important part)
    data_dict['merchant_summary'] = compute_merchant_metric(spark, 
        data_dict['merchant_sales'], data_dict['merchants'])
    
    data_dict['merchant_region_count'] = compute_merchant_region(spark,
        data_dict['merchant_consumer'], data_dict['consumer_region'])

    data_dict['merchant_customer_income'] = compute_merchant_customer_income(spark,
        data_dict['merchant_consumer'], data_dict['consumer_region_income'])
    
    data_dict['merchant_returning_customer'] = compute_returning_customer(spark,
        data_dict['merchant_consumer'])
    
    data_dict['merchant_vip_customer'] = compute_vip_customer(spark,
        data_dict['merchant_consumer'], data_dict['merchant_returning_customer'])
    
    # Join all metrics to form curated merchant dataset
    data_dict['curated'] = data_dict['merchant_summary'].join(
        data_dict['merchant_region_count'],
        'merchant_abn',
        'left'
    ).join(
        data_dict['merchant_customer_income'],
        'merchant_abn',
        'left'
    ).join(
        data_dict['merchant_returning_customer'],
        'merchant_abn',
        'left'
    ).join(
        data_dict['merchant_vip_customer'],
        'merchant_abn',
        'left'
    )
        
                                                                            
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


def compute_merchant_consumer(spark: SparkSession, transaction_df: DataFrame) -> DataFrame:
    return transaction_df \
        .groupby(['merchant_abn', 'user_id']) \
        .agg({'dollar_value':'sum', 'order_id':'count'}) \
        .withColumnRenamed('sum(dollar_value)', 'dollar_spent') \
        .withColumnRenamed('count(order_id)', 'no_orders')


def compute_consumer_region(spark: SparkSession, consumers: DataFrame, 
                            postcodes: DataFrame, user_mapping: DataFrame) -> DataFrame:
    
    return consumers.select(
            ['consumer_id','postcode']
        ).join(
            postcodes, 
            'postcode', 
            'left'
        ).withColumn(
            'sa2_code', 
            F.col('sa2_code').cast(IntegerType())
        ).join(
            user_mapping, 
            'consumer_id', 
            'left'
        )


def compute_region_income(spark: SparkSession, consumer_region: DataFrame,
                         census: DataFrame) -> DataFrame:
    
    return consumer_region.join(
                census.select([
                    'sa2_code',
                    'median_tot_prsnl_inc_weekly'
                ]), 
                'sa2_code',
                'left'
            ).groupby(
                'user_id'
            ).agg(
                {'median_tot_prsnl_inc_weekly':'mean'}
            ).withColumnRenamed(
                'avg(median_tot_prsnl_inc_weekly)', 
                'median_weekly_income'
            )
    
    
def compute_merchant_region(spark: SparkSession, merchant_consumer: DataFrame,
                           consumer_region: DataFrame) -> DataFrame:
    
    return merchant_consumer.select([
            'merchant_abn', 
            'user_id'
        ]).join(
            consumer_region, 
            'user_id', 
            'left'
        ).groupby(
            'merchant_abn'
        ).agg(
            F.countDistinct('sa2_code').alias('sa2_region_count')
        )

def compute_merchant_customer_income(spark: SparkSession, merchant_consumer: DataFrame,
                           consumer_region_income: DataFrame) -> DataFrame:
    
    return merchant_consumer.select([
            'merchant_abn', 
            'user_id'
        ]).join(
            consumer_region_income, 
            'user_id', 
            'left'
        ).groupby(
            'merchant_abn'
        ).agg(
            F.mean(F.col('median_weekly_income')).alias('median_customer_income')
        )

def compute_returning_customer(spark: SparkSession, 
                               merchant_consumer: DataFrame) -> DataFrame:
    
    return merchant_consumer.groupby(
            'merchant_abn'
        ).agg(
            F.count(
                    F.when(F.col('no_orders')>2, True)
                ).alias(
                    'returning_customer'
                ),
            F.mean(F.col('dollar_spent')).alias('mean_spending'),
            F.stddev(F.col('dollar_spent')).alias('std_spending')
        )


def compute_vip_customer(spark: SparkSession, merchant_consumer: DataFrame,
                        merchant_statistics: DataFrame) -> DataFrame:

    return merchant_consumer.join(
        merchant_statistics, 
        'merchant_abn',
        'left'
    ).groupby(
        'merchant_abn'
    ).agg(
        F.count(
            F.when(
                (F.col('dollar_spent') > 100) &
                (F.col('dollar_spent') > F.col('mean_spending') + 2 * F.col('std_spending')),
                True
            )
        ).alias(
            'vip_customer'
        )
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
    )
    
    return merchant_daily_sales.withColumn(
        'avg_daily_commission', 
        F.col('avg_daily_rev') * (F.col('take_rate')/100)
    ).withColumn(
        'avg_commission_per_order',
        F.col('avg_value_per_order') * (F.col('take_rate')/100)
    )

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