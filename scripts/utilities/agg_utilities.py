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
    'total': ('tot', F.sum),
    'sum': ('tot', F.sum),
    'average': ('avg', F.avg),
    'mean': ('avg', F.mean),
    'count': ('num', F.count),
    'count_distinct': ('numd', F.count_distinct),
    'stddev': ('stddev', F.stddev),
    'commission': (
        'commission',
        lambda colname: F.col(colname) * (F.col('take_rate')/100)
    )
}

def group_and_aggregate(df: DataFrame, group_cols: list, 
        agg_dict: dict, suffix: str = '') -> DataFrame:
    """ Aggregating dataframe with specified groupby columns

    Args:
        df (`DataFrame`): Dataframe to be aggregated
        group_cols (list): Features/Attributes to be grouped by
        agg_dict (dict): Features and statistics to be aggregated by
    Returns:
        `DataFrame`: Aggregated `DataFrame` with relevant statistics
    """  
    agg_cols = []

    if len(suffix) > 0:
        suffix = f'_{suffix}'

    for colname, agg_funcs in agg_dict.items():
        print(colname)
        for func_or_name in agg_funcs:
            print(func_or_name)
            prefix = 'ERROR'
            func = lambda _: F.lit('ERROR')

            if type(func_or_name) == str:
                prefix, func = AGGREGATION_FUNCTIONS[func_or_name]
            else:
                prefix, func = func_or_name

            if colname[0:len(prefix)] == prefix or len(prefix) == 0:
                prefix = ''
            else:
                prefix = f'{prefix}_'

            agg_cols.append(
                func(colname).alias(f'{prefix}{colname}{suffix}')
            )

    print(agg_cols)

    return df.groupby(*group_cols).agg(*agg_cols)

def autocalculate_commission(df: DataFrame) -> DataFrame:
    """ Calculating the commissions

    Args:
        df (`DataFrame`): Curated merchant dataset
    Returns:
        `DataFrame`: `DataFrame` with computed commissions
    """

    add_cols = {}
    for colname in df.columns:

        if 'value' not in colname: continue

        new_col = F.col(colname) * (F.col('take_rate')/100)
        if colname[0:6] == 'stddev': new_col *= (F.col('take_rate')/100)

        add_cols[f'commission_{colname}'] = new_col

    return df.withColumns(add_cols)

def compute_aggregates(spark: SparkSession, data_dict: 'defaultdict[str]'):
    """ Computing all data aggregations to generate required metrics

    Args:
        spark (`SparkSession`): Spark session reading the data.
        data_dict (defaultdict[str]): Dictionary to store aggregated datasets
    Returns:
        data_dict (defaultdict[str]): Dictionary to store aggregated datasets
    """

    # Staging aggregates
    logger.info('Computing transactions_with_fraud')
    data_dict['*transactions_with_fraud'] = compute_transactions_with_fraud(
        spark, data_dict['transactions']
    )
    
    logger.info('Computing merchant_daily_sales')
    data_dict['*merchant_daily_sales'] = compute_merchant_daily_sales(spark,
        data_dict['*transactions_with_fraud'])

    logger.info('Computing merchant_monthly_sales')
    data_dict['merchant_monthly_sales'] = compute_merchant_monthly_sales(spark,
        data_dict['*transactions_with_fraud'])

    # logger.info('Computing customer_accounts')
    # data_dict['customer_accounts'] = compute_customer_accounts(spark,
    #     data_dict['consumers'], data_dict['consumer_user_mappings'])
    
    logger.info('Computing merchant_consumers')
    data_dict['*merchant_consumers'] = compute_merchant_consumers(spark,
        data_dict['transactions'])
    
    logger.info('Computing consumer_regions')
    data_dict['*consumer_regions'] = compute_consumer_regions(spark, 
        data_dict['consumers'], data_dict['postcodes'], data_dict['consumer_user_mappings'])
    
    logger.info('Computing consumer_region_incomes')
    data_dict['*consumer_region_incomes'] = compute_region_incomes(spark, 
        data_dict['*consumer_regions'], data_dict['census'])
    
    # Metrics aggregates to be joined (the important part)
    logger.info('Computing merchant_metrics')
    data_dict['*merchant_metrics'] = compute_merchant_metrics(spark, 
        data_dict['merchants'], data_dict['*transactions_with_fraud'],
        data_dict['*merchant_daily_sales'], data_dict['merchant_monthly_sales'])
    
    logger.info('Computing merchant_region_counts')
    data_dict['*merchant_region_counts'] = compute_merchant_regions(spark,
        data_dict['*merchant_consumers'], data_dict['*consumer_regions'])

    logger.info('Computing merchant_customer_incomes')
    data_dict['*merchant_customer_incomes'] = compute_merchant_customer_incomes(spark,
        data_dict['*merchant_consumers'], data_dict['*consumer_region_incomes'])
    
    logger.info('Computing merchant_returning_customers')
    data_dict['*merchant_returning_customers'] = compute_returning_customers(spark,
        data_dict['*merchant_consumers'])
    
    logger.info('Computing merchant_vip_customers')
    data_dict['*merchant_vip_customers'] = compute_vip_customers(spark,
        data_dict['*merchant_consumers'], data_dict['*merchant_returning_customers'])
    
    # Join all metrics to form curated merchant dataset
    logger.info('Computing final_merchant_statistics')
    data_dict['final_merchant_statistics'] = compute_final_merchant_statistics(
        spark, data_dict['*merchant_metrics'],
        data_dict['*merchant_region_counts'], 
        data_dict['*merchant_customer_incomes'],
        data_dict['*merchant_returning_customers'],
        data_dict['*merchant_vip_customers']
    )

    return data_dict # redundant, but return it just in case


def compute_daily_user_transactions(spark: SparkSession,
        transactions_df: DataFrame) -> DataFrame:
    """  Data aggregation to compute daily user transactions

    Args:
        df (`DataFrame`): Transaction dataset
    Returns:
        `DataFrame`: `DataFrame` that contains the total spending, average spending and number of orders
            per user at a specific date
    """         
  
    return group_and_aggregate(
        transactions_df,
        ['user_id', 'order_datetime'],
        {
            'dollar_value': ['total', 'mean'],
            'order_id': ['count']
        }
    )

    return transactions_df.groupby(['user_id', 'order_datetime']) \
        .agg(
            F.sum('dollar_value').alias('total_value'),
            F.count('order_id').alias('num_orders'),
            F.mean('dollar_value').alias('avg_order_value')
        )

def compute_transactions_with_fraud(spark: SparkSession,
        transactions_df:DataFrame) -> DataFrame:
    """ Predicting the fraud rate of daily user transactions with our fraud rate model

    Args:
        spark (`SparkSession`): Spark session reading the data.
        transactions_df (`DataFrame`): Transactions dataset
    Returns:
        `DataFrame`: `DataFrame` with predicted fraud rate of transaction
    """

    # TODO: add model path
    daily_user_transaction_df = compute_daily_user_transactions(
        spark, transactions_df)
    daily_user_transaction_fraud_df = predict_fraud(daily_user_transaction_df)

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
    """ Calculating total daily sales per merchant with fraud rate model

    Args:
        df (`DataFrame`): Curated transaction `DataFrame` with fraud rate model
    Returns:
        `DataFrame`: `DataFrame` which comprises of total daily sales revenue per merchant
    """  
    return group_and_aggregate(
        transaction_with_fraud_df,
        ['merchant_abn', 'order_datetime'],
        {
            'dollar_value': ['sum'],
            'discounted_value': ['sum'],
            'order_id': ['count'],
            'num_fraud_order': [('', lambda colname: F.sum('fraud_prob') / 100)]
        }
    )

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
    """ Calculating monthly sales per merchant with fraud rate model

    Args:
        df (`DataFrame`): Curated transaction `DataFrame` with fraud rate model
    Returns:
        `DataFrame`: Resulting dataframe.
    """ 
    return group_and_aggregate(
        transaction_with_fraud_df,
        [
            'merchant_abn', 
            F.month('order_datetime').alias('order_month'),
            F.year('order_datetime').alias('order_year')
        ],
        {
            'dollar_value': ['sum'],
            'discounted_value': ['sum'],
            'order_id': ['count'],
            'num_fraud_order': [('', lambda colname: F.sum('fraud_prob') / 100)]
        }
    )

    return transaction_with_fraud_df \
        .groupby([
            'merchant_abn', 
            F.month('order_datetime').alias('order_month'),
            F.year('order_datetime').alias('order_year')]) \
        .agg(
            F.sum('dollar_value').alias('sales_revenue'),
            F.count('order_id').alias('num_orders'),
            F.sum('discounted_value').alias('discounted_sales_revenue'),
            (F.sum('fraud_prob') / 100).alias('approximate_fraudulent_orders')
        )
    

def compute_customer_accounts(spark: SparkSession, consumer_df: DataFrame, 
        consumer_user_mapping_df: DataFrame) -> DataFrame:
    """ Mapping each user_id to consumer_id, which is required for merging of datasets

    Args:
        spark (`SparkSession`): Spark session reading the data.
        consumer_df (`DataFrame`): Consumer dataset
        consumer_user_mapping_df (`DataFrame`): Dataset that contains consumer_id for each user_id
    Returns:
        `DataFrame`: Consumer raw `DataFrame` with an extra column user_id
    """     

    return consumer_df.join(
        consumer_user_mapping_df,
        on = 'consumer_id'
    )


def compute_customer_transactions(spark: SparkSession, 
        transactions_df: DataFrame,
        customer_accounts_df: DataFrame) -> DataFrame:
    """ Creates a dataframe that contains customer transaction information

    Args:
        spark (`SparkSession`): Spark session reading the data.
        transactions_df (`DataFrame`): Transactions dataset
        customer_accounts_df (`DataFrame`): Customer accounts dataset
    Returns:
        `DataFrame`: Resulting dataframe.
    """    

    return transactions_df.join(
        customer_accounts_df,
        on='user_id'
    )


def compute_merchant_consumers(spark: SparkSession, transaction_df: DataFrame) -> DataFrame:
    """ Data aggregation, extracting information about total dollar spent and total orders per user for each merchant

    Args:
        spark (`SparkSession`): Spark session reading the data.
        transaction_df (`DataFrame`): Transaction dataset

    Returns:
        `DataFrame`: `Resulting dataframe.
    """

    return group_and_aggregate(
        transaction_df,
        ['merchant_abn', 'user_id'],
        {
            'dollar_value': ['sum'],
            'order_id': ['count']
        }
    )
    
    return transaction_df \
        .groupby(['merchant_abn', 'user_id']) \
        .agg({'dollar_value':'sum', 'order_id':'count'}) \
        .withColumnRenamed('sum(dollar_value)', 'dollar_spent') \
        .withColumnRenamed('count(order_id)', 'num_orders')


def compute_consumer_regions(spark: SparkSession, consumer_df: DataFrame, 
                            postcode_df: DataFrame, user_mapping: DataFrame) -> DataFrame:   
    """ Joining consumer dataset with external dataset, which is SA2 region level by postcode

    Args:
        spark (`SparkSession`): Spark session reading the data.
        consumer_df (`DataFrame`): Consumer dataset
        postcode_df (`DataFrame`): External dataset (SA2 region)
        user_mapping (`DataFrame`): Dataset that helps mapping user_id and consumer_id
    Returns:
        `DataFrame`: `Resulting dataframe.
    """ 

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
    """ Computing the median total personal weekly income per customer, based on the regions they're from

    Args:
        spark (`SparkSession`): Spark session reading the data.
        consumer_region_df (`DataFrame`): Dataset of consumers with their regions
        census_df (`DataFrame`): Census dataset at SA2 region level

    Returns:
        `DataFrame`: `Resulting dataframe.
    """ 
       
    # Median imputation for user with missing weekly income
    imputer = Imputer(
        inputCols = ["avg_median_tot_prsnl_inc_weekly"],
        outputCols = ["avg_median_tot_prsnl_inc_weekly"]
    ).setStrategy("median")
    
    # Join user region with region median income, 
    # if user has multiple SA2 region, find their mean weekly income
    # consumer_region_incomes_df = consumer_region_df.join(
    #     census_df.select([
    #         'sa2_code',
    #         'median_tot_prsnl_inc_weekly'
    #     ]), 
    #     'sa2_code',
    #     'left'
    # ).groupby(
    #     'user_id'
    # ).agg(
    #     {'median_tot_prsnl_inc_weekly':'mean'}
    # ).withColumnRenamed(
    #     'avg(median_tot_prsnl_inc_weekly)', 
    #     'median_weekly_income'
    # )

    consumer_region_incomes_df = group_and_aggregate(
        consumer_region_df.join(
            census_df.select([
                'sa2_code',
                'median_tot_prsnl_inc_weekly'
            ]), 
            'sa2_code',
            'left'
        ),
        ['user_id'],
        {
            'median_tot_prsnl_inc_weekly': ['mean']
        }
    )
    
    return imputer.fit(
        consumer_region_incomes_df
    ).transform(
        consumer_region_incomes_df
    )

    
def compute_merchant_regions(spark: SparkSession, merchant_consumer_df: DataFrame,
                           consumer_region_df: DataFrame) -> DataFrame:
    """ Computing the number of distinct regions each merchant sells to

    Args:
        spark (`SparkSession`): Spark session reading the data.
        merchant_consumer_df (`DataFrame`): Joined transaction dataset that contains merchant and consumer info
        consumer_region_df (`DataFrame`): Joined consumer dataset with external SA2 region level dataset

    Returns:
        `DataFrame`: `Resulting dataframe.
    """

    return group_and_aggregate(
        merchant_consumer_df.select([
            'merchant_abn', 
            'user_id'
        ]).join(
            consumer_region_df, 
            'user_id', 
            'left'
        ),
        ['merchant_abn'],
        {
            'sa2_code': ['count_distinct']
        }
    )

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
    """ Computing the average median weekly income of the merchants' consumer base

    Args:
        spark (`SparkSession`): Spark session reading the data.
        merchant_consumer_df (`DataFrame`): Joined transaction dataset that contains merchant and consumer info
        consumer_region_df (`DataFrame`): Joined consumer dataset with external SA2 region level dataset

    Returns:
        `DataFrame`: `Resulting dataframe.
    """  

    return group_and_aggregate(
        merchant_consumer_df.select([
            'merchant_abn', 
            'user_id'
        ]).join(
            consumer_region_income_df, 
            'user_id', 
            'left'
        ),
        ['merchant_abn'],
        {
            'avg_median_tot_prsnl_inc_weekly': ['mean']
        }
    ).withColumnRenamed(
        'avg_median_tot_prsnl_inc_weekly',
        'median_weekly_income'
    )

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
    """ Computing the number of returning customers for each merchant

    Args:
        spark (`SparkSession`): Spark session reading the data.
        merchant_consumer_df (`DataFrame`): Joined transaction dataset that contains merchant and consumer info

    Returns:
        `DataFrame`: `Resulting dataframe.
    """    

    return group_and_aggregate(
        merchant_consumer_df,
        ['merchant_abn'],
        {
            'returning': [(
                '',
                lambda _: F.count(F.when(F.col('num_order_id')>2, True))
            )],
            'unique': [(
                '',
                lambda _: F.count_distinct('user_id')
            )],
            'tot_dollar_value': ['mean', 'stddev']
        },
        'customers'
    )

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
    """ Computing the number of VIP customers for each merchant based on a few criteria

    Args:
        spark (`SparkSession`): Spark session reading the data.
        merchant_consumer_df (`DataFrame`): Joined transaction dataset that contains merchant and consumer info
        merchant_returning_customer_df (`DataFrame`): Merchant dataset with returning customers column
    Returns:
        `DataFrame`: `Resulting dataframe.
    """

    return group_and_aggregate(
        merchant_consumer_df.join(
            merchant_returning_customer_df, 
            'merchant_abn',
            'left'
        ),
        ['merchant_abn'],
        {
            'vip_customers': [
                (
                    '',
                    lambda _: F.count(
                        F.when(
                            (F.col('tot_dollar_value') > 100) &
                            (
                                F.col('tot_dollar_value') > 
                                F.col('avg_tot_dollar_value_customers') 
                                + 2 * F.col('stddev_tot_dollar_value_customers')
                            ),
                            True
                        )
                    )
                ),
            ]
        }
    )

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

def compute_merchant_metrics(spark: SparkSession, merchant_df: DataFrame, 
        transaction_with_fraud_df: DataFrame,
        merchant_daily_sales_df: DataFrame, 
        merchant_monthly_sales_df: DataFrame) -> DataFrame:
    """ Computing metrics to determine the ranking of merchants

    Args:
        spark (`SparkSession`): Spark session reading the data.
        merchant_df (`DataFrame`): Merchant dataset
        transaction_with_fraud_df (`DataFrame`): Transaction dataset with fraud probability
        merchant_daily_sales_df (`DataFrame`): Merchant dataset with total daily sales per merchant with fraud rate model
        merchant_monthly_sales_df_df (`DataFrame`): Merchant dataset with total monthly sales per merchant with fraud rate model
    Returns:
        `DataFrame`: Dataframe with metrics generated.
    """    
    # first, get overall/per order statistics
    logger.info('first, get overall/per order statistics')
    merchant_per_order_sales_df = group_and_aggregate(
        transaction_with_fraud_df,
        ['merchant_abn'],
        {
            'dollar_value': ['sum', 'mean', 'stddev'],
            'discounted_value': ['sum', 'mean', 'stddev'],
            'order_id': ['count'],
            'fraud_order': [
                ('num', lambda colname: F.sum('fraud_prob') / 100),
                (
                    'rate', 
                    lambda colname: 
                        F.sum('fraud_prob') / (100 * F.count('order_id'))
                ),
            ]
        }
    )
    #         # F.sum('sales_revenue').alias('sales_revenue'),
    #         # F.sum('discounted_sales_revenue') \
    #         #     .alias('discounted_sales_revenue'),
    #         # F.sum('num_orders').alias('num_orders'),
    #         # F.sum('approximate_fraudulent_orders') \
    #         #     .alias('approximate_fraudulent_orders'),
    #         # (F.sum('sales_revenue') / F.sum('num_orders')) \
    #         # .alias('avg_value_per_order'),
    #         # (F.sum('discounted_sales_revenue') / F.sum('num_orders')) \
    #         #     .alias('discounted_avg_value_per_order'),


    # This part is taking a while 
    date_range = merchant_daily_sales_df.select(F.min("order_datetime"), 
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
            merchant_df.select('merchant_abn')
        )

    # # This part is taking a while 
    # month_range = merchant_monthly_sales.select(F.min("order_month"), 
    #                                    F.max("order_month")
    #                                   ).first()
    
    # min_month, max_month = month_range
    
    num_months = max_date.month - min_date.month + 1
    num_months += 12 * (max_date.year - min_date.year)

    # assign the month range to each merchant
    merchant_month_range_df = date_range_df \
        .select(
            F.month('order_datetime').alias('order_month'),
            F.year('order_datetime').alias('order_year')
        ).distinct() \
        .crossJoin(
            merchant_df.select('merchant_abn')
        )


    logger.info('compute daily sales aggregates')
    merchant_daily_sales_df = group_and_aggregate(
        merchant_day_range_df.join(
            merchant_daily_sales_df,
            on = ['order_datetime', 'merchant_abn'],
            how = 'leftouter'
        ).fillna(0),
        ['merchant_abn'],
        {
            'tot_dollar_value': ['mean', 'stddev'],
            'tot_discounted_value': ['mean', 'stddev'],
            'num_order_id': ['mean', 'stddev'],
            'num_fraud_order': ['mean'],
        },
        'daily'
    )

    # Group first to reduce the table size before joining
    # merchant_daily_sales = merchant_day_range_df \
    #     .join(
    #         merchant_daily_sales_df,
    #         on = ['order_datetime', 'merchant_abn'],
    #         how = 'leftouter'
    #     ) \
    #     .fillna(0) \
    #     .groupby('merchant_abn') \
    #     .agg(
    #         # F.sum('sales_revenue').alias('sales_revenue'),
    #         # F.sum('discounted_sales_revenue') \
    #         #     .alias('discounted_sales_revenue'),
    #         # F.sum('num_orders').alias('num_orders'),
    #         # F.sum('approximate_fraudulent_orders') \
    #         #     .alias('approximate_fraudulent_orders'),
    #         # (F.sum('sales_revenue') / F.sum('num_orders')) \
    #         # .alias('avg_value_per_order'),
    #         # (F.sum('discounted_sales_revenue') / F.sum('num_orders')) \
    #         #     .alias('discounted_avg_value_per_order'),
    #         (F.sum('sales_revenue') / num_days).alias('avg_daily_rev'),
    #         (F.sum('discounted_sales_revenue') / num_days) \
    #             .alias('discounted_avg_daily_rev'),
    #         (F.sum('num_orders') / num_days).alias('avg_daily_orders'),
    #         (F.sum('approximate_fraudulent_orders') / num_days) \
    #             .alias('avg_daily_approximate_fraudulent_orders'),
    #         F.stddev('sales_revenue').alias('std_daily_revenue'),
    #         F.stddev('discounted_sales_revenue') \
    #             .alias('std_daily_discounted_revenue'),
    #     )

    logger.info('compute monthly sales aggregates')
    merchant_monthly_sales_df = group_and_aggregate(
        merchant_month_range_df.join(
            merchant_monthly_sales_df,
            on = ['order_month', 'order_year', 'merchant_abn'],
            how = 'leftouter'
        ).fillna(0),
        ['merchant_abn'],
        {
            'tot_dollar_value': ['mean', 'stddev'],
            'tot_discounted_value': ['mean', 'stddev'],
            'num_order_id': ['mean', 'stddev'],
            'num_fraud_order': ['mean'],
        },
        'monthly'
    )

    # Group first to reduce the table size before joining
    # merchant_monthly_sales_df = merchant_month_range_df \
    #     .join(
    #         merchant_monthly_sales_df,
    #         on = ['order_month', 'order_year', 'merchant_abn'],
    #         how = 'leftouter'
    #     ) \
    #     .fillna(0) \
    #     .groupby('merchant_abn') \
    #     .agg(
    #         (F.sum('sales_revenue') / num_months).alias('avg_monthly_rev'),
    #         (F.sum('discounted_sales_revenue') / num_months) \
    #             .alias('discounted_avg_monthly_rev'),
    #         (F.sum('num_orders') / num_months).alias('avg_monthly_orders'),
    #         (F.sum('approximate_fraudulent_orders') / num_months) \
    #             .alias('avg_monthly_approximate_fraudulent_orders'),
    #         F.stddev('sales_revenue').alias('std_monthly_revenue'),
    #         F.stddev('discounted_sales_revenue') \
    #             .alias('std_monthly_discounted_revenue'),
    #     )

    merchant_metrics_df = merchant_df.join(
        merchant_per_order_sales_df,
        on=["merchant_abn"],
        how='left'
    ).join(
        merchant_monthly_sales_df,
        on=["merchant_abn"],
        how='left'
    ).join(
        merchant_daily_sales_df,
        on=["merchant_abn"],
        how='left'
    )
        
    return autocalculate_commission(
        merchant_metrics_df
    )

    # return merchant_metrics_df.withColumns(
    #         {
    #             'avg_daily_commission': F.col('avg_daily_rev') * (F.col('take_rate')/100),
    #             'discounted_avg_daily_commission': F.col('discounted_avg_daily_rev') * (F.col('take_rate')/100),
    #             'avg_monthly_commission': F.col('avg_monthly_rev') * (F.col('take_rate')/100),
    #             'discounted_avg_monthly_commission': F.col('discounted_avg_monthly_rev') * (F.col('take_rate')/100),
    #             'avg_commission_per_order': F.col('avg_value_per_order') * (F.col('take_rate')/100),
    #             'discounted_avg_commission_per_order': F.col('discounted_avg_value_per_order') * (F.col('take_rate')/100),
    #             'overall_commission': F.col('sales_revenue') * (F.col('take_rate')/100),
    #             'discounted_overall_commission': F.col('discounted_sales_revenue') * (F.col('take_rate')/100),
    #             'overall_fraud_rate': F.col('approximate_fraudulent_orders') / F.col('num_orders')
    #         }
    #     )

def compute_final_merchant_statistics(spark: SparkSession, 
        merchant_metrics: DataFrame, 
        merchant_region_counts: DataFrame, 
        merchant_customer_incomes: DataFrame, 
        merchant_returning_customers: DataFrame,
        merchant_vip_customers: DataFrame) -> DataFrame:
    """ A finalizing dataframe that contains all required metrics for merchant ranking

    Args:
        spark (`SparkSession`): Spark session reading the data.
        merchant_metrics (`DataFrame`): Incomplete metrics dataset
        merchant_region_counts (`DataFrame`): Merchant dataset with number of regions each merchant sells to
        merchant_customer_incomes (`DataFrame`): Merchant dataset with 
        merchant_returning_customers (`DataFrame`): Merchant dataset with average median weekly income of the merchants' consumer base
        merchant_vip_customer (`DataFrame`): Merchant dataset with number of vip customers for each merchant
    Returns:
        `DataFrame`: Resulting dataframe.
    """ 

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