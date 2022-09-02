import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.shell import spark
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
import matplotlib.pyplot as plt
import re
from nltk.metrics.distance import jaccard_distance
from nltk.metrics.distance import edit_distance
from nltk.util import ngrams
from sklearn.feature_extraction.text import TfidfVectorizer
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.corpus import words
from nltk.stem import WordNetLemmatizer
import nltk 
nltk.download('words')
from autocorrect import Speller
import logging


# preprocess the data, with the following pipeline
from nltk.metrics.distance import jaccard_distance
from nltk.metrics.distance import edit_distance
from nltk.util import ngrams
from sklearn.feature_extraction.text import TfidfVectorizer
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.corpus import words
from nltk.stem import WordNetLemmatizer
import nltk 
nltk.download('words')
from autocorrect import Speller

# use a class notion so only need to init corpus words and lemmatizer once
# API: preprocess, which takes a single entry of tag, and returns preprocessed tag
class Preprocessor:
    def __init__(self, correct_method="auto"):
        self.lemmatizer = WordNetLemmatizer()
        self.correct_words = words.words()
        self.correct_method = correct_method

        if correct_method == "auto":
            self.auto_corrector = Speller(lang='en')
    
    # jaccard distance is better for minor typos
    def __correct_spelling__(self,word):
        # match the first character
        if self.correct_method == "jaccard":
            similarity_list = [(jaccard_distance(set(ngrams(word, 2)),set(ngrams(w, 2))),w) for w in self.correct_words if w[0]==word[0]]
            similarity_list = sorted(similarity_list, key = lambda val:val[0])
            return similarity_list[0][1]
        if self.correct_method == "edit_distance":
            similarity_list = [(edit_distance(word,w),w) for w in self.correct_words if w[0]==word[0]]
            similarity_list = sorted(similarity_list, key = lambda val:val[0])
            return similarity_list[0][1]
        else:
            return self.auto_corrector(word)


    # case standardization -> puncuation, number removal -> 
    # tokenize -> spelling correction -> lemmatization -> minimum length
    def preprocess(self,tag):
        #case standardization
        tag = tag.lower()
        #puncuation, number removal
        tag= re.sub(r'[^a-zA-Z\s]', '',tag)
        #tokenize 
        tag = word_tokenize(tag)

        #correct spelling
        tag = [self.__correct_spelling__(word) for word in tag ]
        #lemmatization
        # tag = [self.lemmatizer.lemmatize(word) for word in tag]

        #minimum length of 2
        tag = [word for word in tag if len(word) > 2]

        #join the word
        tag = " ".join(word for word in tag)
        return tag

def extract_tags(arr, category="tags"):

    # Split tags into the three components
    arr = arr[1:-1]
    split_arr = re.split("\), \(|\], \[", arr.strip("[()]"))

    if category == "take_rate":
        return re.findall("[\d\.\d]+", split_arr[2])[0]

    elif category == "revenue_level":
        return split_arr[1].lower()

    return split_arr[0].lower()


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("MAST30034 Project 2")
        .config("spark.sql.repl.eagerEval.enabled", True)
        .config("spark.sql.parquet.cacheMetadata", "true")
        .config("spark.sql.session.timeZone", "Etc/UTC")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    sdf = spark.read.parquet("../data/tables/tbl_merchants.parquet")
    df = sdf.toPandas()