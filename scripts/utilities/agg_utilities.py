''' Provide functionality to aggregate datasets.
TODO: commenting on this
'''
from collections import defaultdict
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import Imputer
# from pyspark.pandas import date_range
import pandas as pd

from utilities.model_utilities import predict_fraud
from utilities.print_utilities import print_dataset_summary
from utilities.log_utilities import logger

# The list of any aggregation functions that I could possible need
AGGREGATION_FUNCTIONS = {
    'total': ('tot_', F.sum),
    'average': ('avg_', F.avg),
    'count': ('num_', F.count),
}

def compute_aggregates(spark: SparkSession, data_dict: 'defaultdict[str]'):
    
    # Staging aggregates
    logger.info('Computing transactions_with_fraud')
    data_dict['*transactions_with_fraud'] = compute_transactions_with_fraud(
        spark, data_dict['transactions']
    )
    
    logger.info('Computing merchant_daily_sales')
    data_dict['merchant_daily_sales'] = compute_merchant_daily_sales(spark,
        data_dict['*transactions_with_fraud'])

    logger.info('Computing merchant_monthly_sales')
    data_dict['merchant_monthly_sales'] = compute_merchant_monthly_sales(spark,
        data_dict['*transactions_with_fraud'])

    logger.info('Computing customer_accounts')
    data_dict['customer_accounts'] = compute_customer_accounts(spark,
        data_dict['consumers'], data_dict['consumer_user_mappings'])
    
    logger.info('Computing merchant_consumers')
    data_dict['merchant_consumers'] = compute_merchant_consumers(spark,
        data_dict['transactions'])
    
    logger.info('Computing consumer_regions')
    data_dict['consumer_regions'] = compute_consumer_regions(spark, 
        data_dict['consumers'], data_dict['postcodes'], data_dict['consumer_user_mappings'])
    
    logger.info('Computing consumer_region_incomes')
    data_dict['consumer_region_incomes'] = compute_region_incomes(spark, 
        data_dict['consumer_regions'], data_dict['census'])
    
    # Metrics aggregates to be joined (the important part)
    logger.info('Computing merchant_metrics')
    data_dict['merchant_metrics'] = compute_merchant_metrics(spark, 
        data_dict['merchant_daily_sales'], data_dict['merchant_monthly_sales'], 
        data_dict['merchants'])
    
    logger.info('Computing merchant_region_counts')
    data_dict['merchant_region_counts'] = compute_merchant_regions(spark,
        data_dict['merchant_consumers'], data_dict['consumer_regions'])

    logger.info('Computing merchant_customer_incomes')
    data_dict['merchant_customer_incomes'] = compute_merchant_customer_incomes(spark,
        data_dict['merchant_consumers'], data_dict['consumer_region_incomes'])
    
    logger.info('Computing merchant_returning_customers')
    data_dict['merchant_returning_customers'] = compute_returning_customers(spark,
        data_dict['merchant_consumers'])
    
    logger.info('Computing merchant_vip_customers')
    data_dict['merchant_vip_customers'] = compute_vip_customers(spark,
        data_dict['merchant_consumers'], data_dict['merchant_returning_customers'])
    
    # Join all metrics to form curated merchant dataset
    logger.info('Computing final_merchant_statistics')
    data_dict['final_merchant_statistics'] = compute_final_merchant_statistics(
        spark, data_dict['merchant_metrics'],
        data_dict['merchant_region_counts'], 
        data_dict['merchant_customer_incomes'],
        data_dict['merchant_returning_customers'],
        data_dict['merchant_vip_customers']
    )

    return data_dict # redundant, but return it just in case


def compute_daily_user_transactions(spark: SparkSession,
        transactions_df: DataFrame) -> DataFrame:
    
    return transactions_df.groupby(['user_id', 'order_datetime']) \
        .agg(
            F.sum('dollar_value').alias('total_value'),
            F.count('order_id').alias('num_orders'),
            F.mean('dollar_value').alias('avg_order_value')
        )

def compute_transactions_with_fraud(spark: SparkSession,
        transactions_df:DataFrame) -> DataFrame:

    # TODO: add model path
    daily_user_transaction_df = compute_daily_user_transactions(
        spark, transactions_df)
    daily_user_transaction_fraud_df = predict_fraud(daily_user_transaction_df)
    # daily_user_merchant_df = transactions \
    #     .groupby(['merchant_abn','order_datetime','user_id']) \
    #     .agg(
    #         F.sum('dollar_value').alias('total_value'),
    #         F.count('order_id').alias('num_orders'),
    #         F.mean('dollar_value').alias('avg_order_value')
    #     )
    return transactions_df.join(
        daily_user_transaction_fraud_df,
        on = ['order_datetime','user_id'],
        how = 'leftouter'
    ).withColumn(
        'discounted_value',
        (1 / 100) * (100 - F.col('fraud_prob')) * F.col('dollar_value'),
    )
    
def compute_merchant_daily_sales(spark: SparkSession, 
        transaction_with_fraud_df: DataFrame) -> DataFrame:
    # TODO:Commenting here

    return transaction_with_fraud_df \
        .groupby(['merchant_abn', 'order_datetime']) \
        .agg(
            F.sum('dollar_value').alias('sales_revenue'),
            F.count('order_id').alias('num_orders'),
            F.sum('discounted_value').alias('discounted_sales_revenue'),
            (F.sum('fraud_prob') / 100).alias('approximate_fraudulent_orders')
        )

def compute_merchant_monthly_sales(spark: SparkSession, 
        transaction_with_fraud_df: DataFrame) -> DataFrame:

    return transaction_with_fraud_df \
        .groupby([
            'merchant_abn', 
            F.month('order_datetime').alias('order_month')]) \
        .agg(
            F.sum('dollar_value').alias('sales_revenue'),
            F.count('order_id').alias('num_orders'),
            F.sum('discounted_value').alias('discounted_sales_revenue'),
            (F.sum('fraud_prob') / 100).alias('approximate_fraudulent_orders')
        )
    
    
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
        .withColumnRenamed('count(order_id)', 'num_orders')


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
                    F.when(F.col('num_orders')>2, True)
                ).alias(
                    'returning_customers'
                ),
            F.count_distinct('user_id').alias('unique_customers'),
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
                'vip_customers'
            )
        )

def compute_merchant_metrics(spark: SparkSession, merchant_daily_sales: DataFrame,
    merchant_monthly_sales: DataFrame, merchants: DataFrame) -> DataFrame:
    
    # This part is taking a while 
    date_range = merchant_daily_sales.select(F.min("order_datetime"), 
                                       F.max("order_datetime")
                                      ).first()
    
    min_date, max_date = (datetime.strptime(date_range[0], "%Y-%m-%d"), 
                          datetime.strptime(date_range[1], "%Y-%m-%d"))
    
    num_days = (max_date - min_date).days

    date_range_df = spark \
        .sql(f'''
            SELECT explode(
                sequence(
                    to_date('{min_date}'), 
                    to_date('{max_date}'), 
                    interval 1 day
                )
            ) AS order_datetime
        ''')

    # assign the day range to each merchant
    merchant_day_range_df = date_range_df \
        .crossJoin(
            merchants.select('merchant_abn')
        )

    # # This part is taking a while 
    # month_range = merchant_monthly_sales.select(F.min("order_month"), 
    #                                    F.max("order_month")
    #                                   ).first()
    
    # min_month, max_month = month_range
    
    num_months = max_date.month - min_date.month + 1


    # assign the month range to each merchant
    merchant_month_range_df = date_range_df \
        .select(
            F.month('order_datetime').alias('order_month')
        ).distinct() \
        .crossJoin(
            merchants.select('merchant_abn')
        )

    
    # Group first to reduce the table size before joining
    merchant_daily_sales = merchant_day_range_df \
        .join(
            merchant_daily_sales,
            on = ['order_datetime', 'merchant_abn'],
            how = 'leftouter'
        ) \
        .fillna(0) \
        .groupby('merchant_abn') \
        .agg(
            F.sum('sales_revenue').alias('sales_revenue'),
            F.sum('discounted_sales_revenue') \
                .alias('discounted_sales_revenue'),
            F.sum('num_orders').alias('num_orders'),
            F.sum('approximate_fraudulent_orders') \
                .alias('approximate_fraudulent_orders'),
            (F.sum('sales_revenue') / num_days).alias('avg_daily_rev'),
            (F.sum('discounted_sales_revenue') / num_days) \
                .alias('discounted_avg_daily_rev'),
            (F.sum('sales_revenue') / F.sum('num_orders')) \
                .alias('avg_value_per_order'),
            (F.sum('discounted_sales_revenue') / F.sum('num_orders')) \
                .alias('discounted_avg_value_per_order'),
            (F.sum('num_orders') / num_days).alias('avg_daily_orders'),
            (F.sum('approximate_fraudulent_orders') / num_days) \
                .alias('avg_daily_approximate_fraudulent_orders'),
            F.stddev('sales_revenue').alias('std_daily_revenue'),
            F.stddev('discounted_sales_revenue') \
                .alias('std_daily_discounted_revenue'),
        )

    # Group first to reduce the table size before joining
    merchant_monthly_sales = merchant_month_range_df \
        .join(
            merchant_monthly_sales,
            on = ['order_month', 'merchant_abn'],
            how = 'leftouter'
        ) \
        .fillna(0) \
        .groupby('merchant_abn') \
        .agg(
            (F.sum('sales_revenue') / num_months).alias('avg_monthly_rev'),
            (F.sum('discounted_sales_revenue') / num_months) \
                .alias('discounted_avg_monthly_rev'),
            (F.sum('num_orders') / num_months).alias('avg_monthly_orders'),
            (F.sum('approximate_fraudulent_orders') / num_months) \
                .alias('avg_monthly_approximate_fraudulent_orders'),
            F.stddev('sales_revenue').alias('std_monthly_revenue'),
            F.stddev('discounted_sales_revenue') \
                .alias('std_monthly_discounted_revenue'),
        )
        
    return merchants \
        .join(
            merchant_monthly_sales,
            on=["merchant_abn"],
            how='left'
        ) \
        .join(
            merchant_daily_sales,
            on=["merchant_abn"],
            how='left'
        ) \
        .withColumns(
            {
                'avg_daily_commission': F.col('avg_daily_rev') * (F.col('take_rate')/100),
                'discounted_avg_daily_commission': F.col('discounted_avg_daily_rev') * (F.col('take_rate')/100),
                'avg_monthly_commission': F.col('avg_monthly_rev') * (F.col('take_rate')/100),
                'discounted_avg_monthly_commission': F.col('discounted_avg_monthly_rev') * (F.col('take_rate')/100),
                'avg_commission_per_order': F.col('avg_value_per_order') * (F.col('take_rate')/100),
                'discounted_avg_commission_per_order': F.col('discounted_avg_value_per_order') * (F.col('take_rate')/100),
                'overall_commission': F.col('sales_revenue') * (F.col('take_rate')/100),
                'discounted_overall_commission': F.col('discounted_sales_revenue') * (F.col('take_rate')/100),
                'overall_fraud_rate': F.col('approximate_fraudulent_orders') / F.col('num_orders')
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