''' Provides functions to save datasets
'''

from collections import defaultdict
import os

import pandas as pd
from pyspark.sql import DataFrame as SDF
from pandas import DataFrame as PDF
from pandas import Series as PS
from geopandas import GeoDataFrame as GDF

from utilities.log_utilities import logger

DEFAULT_OUTPUT_PATH = './data/curated' # where the curated data will be stored

def write_data(data_dict: 'defaultdict[str]', 
        data_path: str = DEFAULT_OUTPUT_PATH):

    # ensure that the path exists
    if not os.path.exists(data_path):
        logger.info(f'`{data_path}` does not exist. Creating the `{data_dict}` directory.')
        os.mkdir(data_path)

    for dataset_name, data in data_dict.items():

        # filename to save the dataset with
        save_name = f'{data_path}/{dataset_name}'

        logger.info(f'saving {save_name}')

        if type(data) == SDF:
            data:SDF = data
            data.write.mode('overwrite').parquet(save_name)
        elif type(data) == PDF:
            data:PDF = data
            data.to_parquet(save_name)
        elif type(data) == PS:
            data:PS = data
            data.to_csv(save_name + '.csv')
        elif type(data) == GDF:
            data:GDF = data
            data.to_file(save_name)
        else: 
            logger.error(
                'you\'ve given me a file format I don\'t know how to save.'
                + ' Given type: ' + str(type(data)))
