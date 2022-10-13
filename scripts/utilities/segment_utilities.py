''' Related to segmenting merchants into industries based on tags'''

from pyspark.sql import DataFrame
import pandas as pd
import numpy as np
import json

DEFAULT_SEGMENT_JSON_PATH = './scripts/utilities' # where the json file is

# TODO: function to segment merchants tags

def read_segment_json(json_path: str = DEFAULT_SEGMENT_JSON_PATH) -> dict:
    """ Read the segment json file into dictionary
    Args:
        json_path ('str'): directory path of the segment json file
    Returns:
        dict: dictionary specifying industries as key to their tags
    """  
    
    with open(f"{json_path}/segment_tags.json", 'r') as json_file:
        segment_tags = json.load(json_file)
        
    return segment_tags


def reverse_dict(ori_dict: dict) -> dict:
    """ Take a dictionary and reverse the key-value relation
    Args:
        ori_dict (dict): original dictionary to reverse key-value relation on
    Returns:
        dict: dictionary with reversed key-value relation
    """  
    
    # Reverse a dictionary key-value relation by mapping each 
    # value to its original key
    new_dict = {}
    for key, items in ori_dict.items():
        for item in items:
            new_dict[item] = key
            
    return new_dict

def get_tag(row, tag_df) -> list:
    """ Take a merchant's row in the dataset and merge all their tags
    into a list.
    Args:
        row ('DataFrame' row): a row in the merchant's dataset
        tag_df ('DataFrame') : dataframe containing merchant abn and their tags in list
    Returns:
        list: list containing all tags that a merchant has
    """  
    
    # Combine all tags a merchant has into a list
    tag_list = []
    for idx in np.flatnonzero(row):
        tag_list.append(tag_df.columns[idx])
        
    return tag_list


def transform_segment(merchants_with_tags: DataFrame) -> DataFrame:
    """ Take the merchants with tags dataframe and transform it into merchants
    with segments dataframe
    Args:
        merchants_with_tags ('DataFrame'): the merchants with tags dataframe
    Returns:
        DataFrame: the merchants with segment dataframe
    """  
    merchants_with_tags = merchants_with_tags.toPandas()
    segment_tags = read_segment_json()
    labelling = reverse_dict(segment_tags)
    
    remove_col = ['name','merchant_abn','take_rate','revenue_level']
    tag_df = merchants_with_tags.iloc[:, ~(merchants_with_tags.columns.isin(remove_col))]
    tag_df = tag_df.apply(lambda x: get_tag(x, tag_df), axis=1)
    
    merchant_segment = {}
    i = 0
    for idx, item in tag_df.items():
        abn = merchants_with_tags.iloc[i]['merchant_abn']
        merchant_segment[abn] = []
        for tag in item:
            if labelling[tag] not in merchant_segment[abn]:
                merchant_segment[abn].append(labelling[tag])

        i += 1
        
    df = (pd.DataFrame.from_dict(merchant_segment,orient='index')
                      .apply(lambda x: ",".join(x.dropna()), axis=1)
                      .reset_index()
                      .rename({'index':'merchant_abn'},axis=1))
    
    return pd.concat([df['merchant_abn'], 
                      df[0].str.get_dummies(sep=',')], 
                     axis=1)


def get_segments_abn(merchants_with_segments: DataFrame) -> dict:
    """ Return all abns associated with each segment in dictionary format
    Args:
        merchants_with_segments ('DataFrame'): the merchants with segments dataframe
    Returns:
        dict: dictionary of segments with their associated merchant abns
    """  
    
    segment_dict = {col:[] for col in merchants_with_segments.columns if col != 'merchant_abn'}
    
    for segment in segment_dict.keys():
        segment_abn = merchants_with_segments[merchants_with_segments[segment] != 0]['merchant_abn'].unique()
        segment_dict[segment] += segment_abn.tolist()
        
    return segment_dict
