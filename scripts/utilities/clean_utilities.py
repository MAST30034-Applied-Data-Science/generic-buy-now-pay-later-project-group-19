''' Provides functions to clean the datasets.
TODO: Commenting run needed.
'''

from collections import defaultdict
import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
import pandas as pd
import numpy as np
from nltk.metrics.distance import jaccard_distance
from nltk.metrics.distance import edit_distance
from nltk.util import ngrams
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.corpus import words
from nltk.stem import WordNetLemmatizer
import nltk 
nltk.download('stopwords')
nltk.download('words')
nltk.download('punkt')
nltk.download('wordnet')
from autocorrect import Speller

from utilities.log_utilities import logger

def clean_data(spark: SparkSession, 
        data_dict: 'defaultdict[str]') -> 'defaultdict[str]':
    """ TODO: Commenting

    Args:
        spark (SparkSession): _description_
        data_dict (defaultdict[str]): _description_

    Returns:
        defaultdict[str]: _description_
    """
    data_dict = remove_transaction_outliers(spark, data_dict)
    data_dict = extract_merchant_tags(spark, data_dict)
    return data_dict


def remove_transaction_outliers(spark: SparkSession, 
        data_dict: 'defaultdict[str]') -> 'defaultdict[str]':
    """ Check outliers from the transaction dataset
    Args:
        data_dict (defaultdict[str]): transaction Spark DataFrame
    Returns:
        `DataFrame`: Resulting `DataFrame`
    """
    
    logger.info('Removing transaction outliers')

    data_dict['transactions'] = data_dict['transactions'].withColumn(
        'log(dollar_value)',
        F.log(F.col('dollar_value'))
    )

    # Find lower and upper bound
    lwr, upr = data_dict['transactions'].approxQuantile(
        'log(dollar_value)', [0.25, 0.75], 0.001)
    iqr = upr - lwr
    lwr_bound = lwr - 1.5 * iqr
    upr_bound = upr + 1.5 * iqr
    
    lwr_bound, upr_bound = np.exp(lwr_bound), np.exp(upr_bound)
    
    # Filter data by lower and upper bound
    new_transaction = data_dict['transactions'].where(
        (F.col('dollar_value') >= lwr_bound) &
        (F.col('dollar_value') <= upr_bound)
    )
    
    # Summary of outlier removal
    logger.info(f"Outlier Removal Summary:")
    pre_removal = data_dict['transactions'].count()
    post_removal = new_transaction.count()
    logger.info(f"New range of dollar per transaction: {lwr_bound:.2f} - {upr_bound:.2f}")
    logger.info(f"Number of instances after outlier removal: {post_removal}")
    logger.info(f"Number of outliers removed: {pre_removal - post_removal}")
    logger.info(f"% data removed: {((pre_removal - post_removal)/pre_removal)*100:.2f}%")
    

    data_dict['transactions'] = new_transaction    
    return data_dict


def extract_merchant_tags(spark: SparkSession, 
        data_dict: 'defaultdict[str]') -> 'defaultdict[str]':

    logger.info('Extracting merchant tags')

    merchants_df = data_dict['merchants'].toPandas()

    def extract_merchant_tag_col(arr, category="tags"):

        # Split tags into the three components
        arr = arr[1:-1]
        split_arr = re.split("\), \(|\], \[", arr.strip("[()]"))

        if category == "take_rate":
            return re.findall("[\d\.\d]+", split_arr[2])[0]

        elif category == "revenue_level":
            return split_arr[1].lower()

        return split_arr[0].lower()

    merchants_df["tag"] = merchants_df.apply(
        lambda row: extract_merchant_tag_col(row.tags, "tags"), axis=1)
    merchants_df["revenue_level"] = merchants_df.apply(
        lambda row: extract_merchant_tag_col(row.tags, "revenue_level"), axis=1)
    merchants_df["take_rate"] = merchants_df.apply(
        lambda row: extract_merchant_tag_col(row.tags, "take_rate"), axis=1)

    tag_col = merchants_df["tag"].copy()

    preprocessor = MerchantPreprocessor()
    for i in range(tag_col.size):
        tag_col[i] = preprocessor.preprocess(tag_col[i])

    # Count and vectorize text in tags with comma as delimiter
    vectorizer = CountVectorizer(tokenizer=lambda text: re.split(',',text))
    X = vectorizer.fit_transform(tag_col)

    # Join the vectorizer with merchant data
    count_vect_df = pd.DataFrame(X.todense(), 
        columns=vectorizer.get_feature_names())
    data_dict['merchants'] = spark.createDataFrame(
        pd.concat([merchants_df, count_vect_df], axis=1)\
            .drop(['tags','tag'], axis=1)
    )
    data_dict['merchant_tags'] = merchants_df["tag"]

    return data_dict


# use a class notion so only need to init corpus words and lemmatizer once
# API: preprocess, which takes a single entry of tag, and returns preprocessed tag
class MerchantPreprocessor:
    def __init__(self, correct_method="auto"):
        self.lemmatizer = WordNetLemmatizer()
#         self.correct_words = words.words()
#         self.correct_method = correct_method
#         self.stopwords = set(stopwords.words('english'))

#         if correct_method == "auto":
#             self.auto_corrector = Speller(lang='en')
    
#     # jaccard distance is better for minor typos
#     def __correct_spelling__(self,word):
#         # match the first character
#         if self.correct_method == "jaccard":
#             similarity_list = [(jaccard_distance(set(ngrams(word, 2)),set(ngrams(w, 2))),w) for w in self.correct_words if w[0]==word[0]]
#             similarity_list = sorted(similarity_list, key = lambda val:val[0])
#             return similarity_list[0][1]
#         if self.correct_method == "edit_distance":
#             similarity_list = [(edit_distance(word,w),w) for w in self.correct_words if w[0]==word[0]]
#             similarity_list = sorted(similarity_list, key = lambda val:val[0])
#             return similarity_list[0][1]
#         else:
#             return self.auto_corrector(word)


    # case standardization -> puncuation, number removal -> 
    # tokenize -> spelling correction -> lemmatization -> minimum length
    def preprocess(self,tag_line):
        #case standardization
        tag_line = tag_line.lower()
        
        #puncuation, number removal, except comma
        tag_line= re.sub(r'[^a-zA-Z\s,]', ' ',tag_line)
        
        #tokenize by comma
        tag_line = tag_line.split(',')
        
        #strip leading and ending of tag
        tag_line = [text.strip() for text in tag_line]

        new_tag_line = []
        for tag in tag_line:

            new_tag = word_tokenize(tag)

            #correct spelling
#             new_tag = [self.__correct_spelling__(text) for text in new_tag ]
        
            #stop word removal
#             new_tag = [text for text in new_tag if text not in self.stopwords]

            #lemmatization
            new_tag = [self.lemmatizer.lemmatize(text) for text in new_tag]

            #minimum length of 2
            new_tag = " ".join([text for text in new_tag if len(text) > 2])
            
            new_tag_line.append(new_tag)

        return ",".join(new_tag_line)



# def extract_merchant_tags(merchant_df: DataFrame) -> DataFrame:
#     """ Extract tags, revenue level and take rate from tags column in the 
#     merchant dataset

#     Args:
#         merchant_df (`DataFrame`): merchant spark dataframe
#     Returns:
#         `DataFrame`: Resulting `DataFrame`
#     """
    
#     # Since merchant df is small, we will use pandas for convenience
#     merchant_df = merchant_df.toPandas()
    
#     def extract(arr: str, category: str = 'tags'):
#         """ Extract tags, revenue level and take rate from tags
#         given in the form '((tags), (revenue_level), (take_rate))'
#         Args:
#             arr (str): array in the tag column
#             category (str): 'tags', 'revenue_level' or 'take_rate'
#         Returns:
#             category_value (str or float): value extracted by the category
#         """

#         # Split tags into the three components
#         arr = arr[1:-1]
#         split_arr = re.split('\), \(|\], \[', arr.strip('[()]'))

#         if category == 'take_rate':
#             return float(re.findall('[\d\.\d]+', split_arr[2])[0])

#         elif category == 'revenue_level':
#             return split_arr[1].lower()

#         # by default return tags
#         return split_arr[0].lower()
    
    
#     # Split information into separate columns
#     for col in ['tag', 'revenue_level', 'take_rate']:
#         merchant_df[col] = merchant_df['tags'].apply(
#             lambda x: extract(x, col)
#         )

#     return merchant_df.drop(
#             'tags',
#             axis=1
#         )