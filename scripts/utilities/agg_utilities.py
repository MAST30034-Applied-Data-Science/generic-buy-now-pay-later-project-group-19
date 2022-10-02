''' Provide functionality to aggregate datasets.
TODO: commenting on this
'''
from collections import defaultdict
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import Imputer
import pandas as pd

from utilities.print_utilities import print_dataset_summary
from utilities.clean_utilities import remove_nan_values
from utilities.log_utilities import logger

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
    
    data_dict['merchant_consumers'] = compute_merchant_consumers(spark,
        data_dict['transactions'])
    
    data_dict['consumer_regions'] = compute_consumer_regions(spark, 
        data_dict['consumers'], data_dict['postcodes'], data_dict['consumer_user_mappings'])
    
    data_dict['consumer_region_incomes'] = compute_region_incomes(spark, 
        data_dict['consumer_regions'], data_dict['census'])
    
    # Metrics aggregates to be joined (the important part)
    data_dict['merchant_metrics'] = compute_merchant_metrics(spark, 
        data_dict['merchant_sales'], data_dict['merchants'])
    
    data_dict['merchant_region_counts'] = compute_merchant_regions(spark,
        data_dict['merchant_consumers'], data_dict['consumer_regions'])

    data_dict['merchant_customer_incomes'] = compute_merchant_customer_incomes(spark,
        data_dict['merchant_consumers'], data_dict['consumer_region_incomes'])
    
    data_dict['merchant_returning_customers'] = compute_returning_customers(spark,
        data_dict['merchant_consumers'])
    
    data_dict['merchant_vip_customers'] = compute_vip_customers(spark,
        data_dict['merchant_consumers'], data_dict['merchant_returning_customers'])
    
    # Join all metrics to form curated merchant dataset
    data_dict['final_merchant_statistics'] = compute_final_merchant_statistics(
        spark, data_dict['merchant_metrics'],
        data_dict['merchant_region_counts'], 
        data_dict['merchant_customer_incomes'],
        data_dict['merchant_returning_customers'],
        data_dict['merchant_vip_customers']
    )

    return data_dict # redundant, but return it just in case

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


def compute_merchant_consumers(spark: SparkSession, transaction_df: DataFrame) -> DataFrame:
    return transaction_df \
        .groupby(['merchant_abn', 'user_id']) \
        .agg({'dollar_value':'sum', 'order_id':'count'}) \
        .withColumnRenamed('sum(dollar_value)', 'dollar_spent') \
        .withColumnRenamed('count(order_id)', 'no_orders')


def compute_consumer_regions(spark: SparkSession, consumer_df: DataFrame, 
                            postcode_df: DataFrame, user_mapping: DataFrame) -> DataFrame:
    
    return consumer_df.select(
            ['consumer_id','postcode']
        ).join(
            postcode_df, 
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

# Join user region with region median income, 
# if user has multiple SA2 region, find their mean weekly income
def compute_region_incomes(spark: SparkSession, consumer_region_df: DataFrame,
                         census_df: DataFrame) -> DataFrame:
    
    # Median imputation for user with missing weekly income
    imputer = Imputer(
        inputCols = ["median_weekly_income"],
        outputCols = ["median_weekly_income"]
    ).setStrategy("median")

    # return consumer_region_df.join(
    #             census_df.select([
    #                 'sa2_code',
    #                 'median_tot_prsnl_inc_weekly'
    #             ]), 
    #             'sa2_code',
    #             'left'
    #         ).groupby(
    #             'user_id'
    #         ).agg(
    #             {'median_tot_prsnl_inc_weekly':'mean'}
    #         ).withColumnRenamed(
    #             'avg(median_tot_prsnl_inc_weekly)', 
    #             'median_weekly_income'
    #         )

    return consumer_region_df.join(
                census_df.select([
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
    
    # Join user region with region median income, 
    # if user has multiple SA2 region, find their mean weekly income
    consumer_region_incomes_df = consumer_region_df.join(
        census_df.select([
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
    
    return imputer.fit(
        consumer_region_incomes_df
    ).transform(
        consumer_region_incomes_df
    )

    
def compute_merchant_regions(spark: SparkSession, merchant_consumer_df: DataFrame,
                           consumer_region_df: DataFrame) -> DataFrame:
    
    return merchant_consumer_df.select([
            'merchant_abn', 
            'user_id'
        ]).join(
            consumer_region_df, 
            'user_id', 
            'left'
        ).groupby(
            'merchant_abn'
        ).agg(
            F.countDistinct('sa2_code').alias('sa2_region_count')
        )

def compute_merchant_customer_incomes(spark: SparkSession, merchant_consumer_df: DataFrame,
                           consumer_region_income_df: DataFrame) -> DataFrame:
    
    return merchant_consumer_df.select([
            'merchant_abn', 
            'user_id'
        ]).join(
            consumer_region_income_df, 
            'user_id', 
            'left'
        ).groupby(
            'merchant_abn'
        ).agg(
            F.mean('median_weekly_income').alias('median_customer_income')
        )

def compute_returning_customers(spark: SparkSession, 
                               merchant_consumer_df: DataFrame) -> DataFrame:
    
    return merchant_consumer_df.groupby(
            'merchant_abn'
        ).agg(
            F.count(
                    F.when(F.col('no_orders')>2, True)
                ).alias(
                    'returning_customer'
                ),
            F.mean('dollar_spent').alias('mean_spending'),
            F.stddev('dollar_spent').alias('std_spending')
        )


def compute_vip_customers(spark: SparkSession, merchant_consumer_df: DataFrame,
                        merchant_returning_customer_df: DataFrame) -> DataFrame:

    return merchant_consumer_df.join(
            merchant_returning_customer_df, 
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

def compute_merchant_metrics(spark: SparkSession, merchant_sales: DataFrame,
                           merchants: DataFrame) -> DataFrame:
    
    
    # This part is taking a while 
    date_range = merchant_sales.select(F.min("order_datetime"), 
                                       F.max("order_datetime")
                                      ).first()
    
    min_date, max_date = (datetime.strptime(date_range[0], "%Y-%m-%d"), 
                          datetime.strptime(date_range[1], "%Y-%m-%d"))
    
    num_days = (max_date - min_date).days


    
    # Group first to reduce the table size before joining
    merchant_daily_sales = merchant_sales \
        .groupby('merchant_abn') \
        .agg(
            F.sum('sales_revenue').alias('sales_revenue'),
            F.sum('no_orders').alias('no_orders'),
            (F.sum('sales_revenue') / num_days).alias('avg_daily_rev'),
            (F.sum('sales_revenue') / F.sum('no_orders')).alias('avg_value_per_order'),
            (F.sum('no_orders') / num_days).alias('avg_daily_order')
        )
    
    merchant_daily_sales = merchants.join(
        merchant_daily_sales, 
        on=["merchant_abn"],
        how='left'
    )
        
    return merchant_daily_sales.withColumns(
        {
            'avg_daily_commission': F.col('avg_daily_rev') * (F.col('take_rate')/100),
            'avg_commission_per_order': F.col('avg_value_per_order') * (F.col('take_rate')/100),
            'overall_commission': F.col('sales_revenue') * (F.col('take_rate')/100),
        }
    )

def compute_final_merchant_statistics(spark: SparkSession, 
        merchant_metrics: DataFrame, 
        merchant_region_counts: DataFrame, 
        merchant_customer_incomes: DataFrame, 
        merchant_returning_customers: DataFrame,
        merchant_vip_customers: DataFrame) -> DataFrame:

    # Join all metrics to form curated merchant dataset
    return merchant_metrics.join(
            merchant_region_counts,
            'merchant_abn',
            'left'
        ).join(
            merchant_customer_incomes,
            'merchant_abn',
            'left'
        ).join(
            merchant_returning_customers,
            'merchant_abn',
            'left'
        ).join(
            merchant_vip_customers,
            'merchant_abn',
            'left'
        )