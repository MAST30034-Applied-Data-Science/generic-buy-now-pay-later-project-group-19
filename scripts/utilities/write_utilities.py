''' Provides functions to save datasets
'''

from collections import defaultdict
import os

import pandas as pd
from pyspark.sql import DataFrame as SDF
from pandas import DataFrame as PDF
from geopandas import GeoDataFrame as GDF

from utilities.log_utilities import logger

DEFAULT_OUTPUT_PATH = './data/curated' # where the curated data will be stored

def write_data(data_dict: 'defaultdict[str]', 
        data_path: str = DEFAULT_OUTPUT_PATH):

    for dataset_name, data in data_dict.items():

        # filename to save the dataset with
        save_name = f'{data_path}/{dataset_name}.parquet'

        if type(data) == SDF:
            data:SDF = data
            data.write.mode('overwrite').parquet(save_name)
        elif type(data) == PDF:
            data:PDF = data
            data.to_parquet(save_name)
        elif type(data) == SDF:
            data.to_parquet(save_name)
        else: 
            logger.error(
                'you\'ve given me a file format I don\'t know how to save.'
                + ' How does this even happen? ' + str(type(data)))
