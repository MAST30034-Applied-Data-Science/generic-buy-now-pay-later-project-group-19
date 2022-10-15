script_description = """ Generates the merchant rankings.
"""

# Python Libraries
import os
import json
import logging
import argparse
from collections import defaultdict
# ... TODO: Add to this as necessary

# External Libraries
import pandas as pd
# ... TODO: Add to this as necessary

# Our Modules
from utilities.log_utilities import logger, file_handler
import utilities.print_utilities as PRINT
import utilities.read_utilities as READ
import utilities.agg_utilities as AGG
import utilities.model_utilities as MODEL
import utilities.write_utilities as WRITE
import utilities.rank_utilities as RANK
# ... TODO: Add to this as necessary

# Constants (these will modify the behavior of the script)
DEFAULT_INPUT_DATA_PATH = WRITE.DEFAULT_OUTPUT_DATA_PATH # where the raw data is
DEFAULT_OUTPUT_RANK_PATH = WRITE.DEFAULT_RANKING_PATH
# ... TODO: Add to this as necessary

################################################################################
# Define the ETL Process
################################################################################
def rank_merchants(input_path:str = DEFAULT_INPUT_DATA_PATH, 
        rank_path:str = DEFAULT_OUTPUT_RANK_PATH):
    """ TODO: commenting.

    Args:
        input_path (str): Path where the curated data is stored.
        rank_path (str): Path where resulting rankings will be saved.

    Returns:
        `LinearRegression`: Output fraud model.
    """

    # read in the segments that Oliver defined
    segments = json.load(open(f'{input_path}/segments.json'))
    logger.debug(segments)

    # create a mapping df real quick
    rows = []

    for seg, abn_list in segments.items():
        if seg == 'repair services': continue
        for abn in abn_list:
            rows.append({'segment': seg, 'merchant_abn': abn})

    segments_df = pd.DataFrame(rows)
    logger.debug(segments_df.head(5))

    # read the curated data
    merchant_df = READ.pd_read_merchant_statistics(input_path)
    logger.debug(merchant_df.head(5))

    rank_cols_desc = [
        # exposure & familiarity features
        'numd_sa2_code', 'returning_customers', 'unique_customers',
        # risk of defaulting on payments
        'median_weekly_income', 
        # financial performance
        'commission_avg_tot_dollar_value_monthly', 'avg_num_order_id_monthly',
        # fraud
        'avg_discounted_value',
    ]

    rank_cols_asc = [
        # financial performance
        'stddev_tot_dollar_value_monthly',
        # Fraud
        'rate_fraud_order', 'stddev_tot_discounted_value_daily',
    ]

    cols_to_keep = ['merchant_abn', 'name', 'tags', 'tag'] \
        + rank_cols_desc + rank_cols_asc

    merchant_df = merchant_df[cols_to_keep]

    # define the columns to add ranks for
    rank_cols_dict = {}
    for col in rank_cols_desc:
        rank_cols_dict[col] = False
    for col in rank_cols_asc:
        rank_cols_dict[col] = True

    # iterate through and rank these columns
    for colname, asc in rank_cols_dict.items():
        merchant_df = RANK.add_column_rank(merchant_df, colname, ascending=asc,
            rank_type = 'minmax')

    # calculate the uwar for the cols to rank
    merchant_df = RANK.average_rank(merchant_df, rank_cols_dict.keys(),
        rank_type = 'minmax')

    # ensure that the path exists
    if not os.path.exists(rank_path):
        logger.info(f'`{rank_path}` does not exist. Creating the `ranking_path` directory.')
        os.mkdir(rank_path)

    merchant_df.to_csv(f'{rank_path}/merchant_rankings.csv')

    # choose the first 100 of these
    final_100_df = merchant_df.head(100)

    # save the rank, abn, name and sales_revenue in a csv
    final_100_df.to_csv(f'{rank_path}/top-100-merchants.csv')

    # segment the merchants
    segmented_merchant_df = merchant_df.join(
        segments_df.set_index('merchant_abn'),
        on = 'merchant_abn',
        how = 'left'
    )

    # define the segmented feature names
    seg_feature_names = {
        'gifts souvenirs': # revenue
            [
                'commission_avg_tot_dollar_value_monthly', 
                'avg_num_order_id_monthly',
                'stddev_tot_dollar_value_monthly',
            ],
        'home furnishings': # risk
            [
                'median_weekly_income', 
            ],
        'leisure goods and services': # exposure
            [
                'numd_sa2_code', 
                'returning_customers', 
                'unique_customers',
            ],
        'tech and telecom': # exposure
            [
                'numd_sa2_code', 
                'returning_customers', 
                'unique_customers',
            ],
    }

    # calculate ranks per segment
    for seg, feature_list in seg_feature_names.items():
        feature_weights = feature_weights = RANK.bias_weights(
            rank_cols_dict.keys(),
            feature_list
        )

        segmented_merchant_df = RANK.average_rank(
            segmented_merchant_df,
            rank_cols_dict.keys(),
            'minmax',
            feature_weights,
            '_'.join(seg.split(' '))
        )

    # iterate over segments, print them, and save them
    for seg in set(segmented_merchant_df['segment']):
        if type(seg) == float: continue
        print(f'\nTop 10 {seg} Merchants')
        print(
            segmented_merchant_df[
                segmented_merchant_df['segment'] == seg
            ].sort_values(
                f"average_rank_{'_'.join(seg.split(' '))}"
            ).head(10)[
                ['merchant_abn', 'name']
            ]
        )

        # save top 10 per segment
        segmented_merchant_df[
            segmented_merchant_df['segment'] == seg
        ].sort_values(
            f"average_rank_{'_'.join(seg.split(' '))}"
        ).head(10).to_csv(f'{rank_path}/top-10-{seg}.csv')


################################################################################
# Functionality to only run in script mode
################################################################################
if __name__ == '__main__':

    ############################################################################
    # Get script parameter(s)
    ############################################################################
    # define the parser
    parser = argparse.ArgumentParser(description=script_description)

    # overwrite the debugging flag
    parser.add_argument('-d', '--debug', '--debugging',
        default=False,
        help='Whether to print debug statements.',
        action='store_true')

    # data input folder
    parser.add_argument('-i', '--input', 
        default=DEFAULT_INPUT_DATA_PATH,
        help='the folder where the curated data is stored.')

    # data output folder
    parser.add_argument('-o', '--output', 
        default=DEFAULT_OUTPUT_RANK_PATH,
        help='the folder where the ranks are stored. Subdirectories may be created.')

    # Parse arguments as necessary
    args = parser.parse_args()
    
    # apply the logger level to logger
    if args.debug:
        logger.setLevel(logging.DEBUG)
        file_handler.setLevel(logging.DEBUG)
        logger.addHandler(file_handler)
    else: 
        logger.setLevel(logging.INFO)
        file_handler.setLevel(logging.INFO)
        logger.addHandler(file_handler)

    # print args to debug
    logger.debug(f'arguments: \n{args}')

    ############################################################################
    # Run the Script
    ############################################################################
    output = rank_merchants(args.input, args.output)    

    logger.info('Merchant Ranking Complete!')