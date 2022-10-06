''' Related to generating and using models for fraud prediction.
TODO: commenting
'''

from pyspark.sql import DataFrame
from pyspark.ml.regression import LinearRegressionModel as LRM
from pyspark.ml.feature import VectorAssembler

DEFAULT_MODEL_PATH = './models' # where the raw data is

# TODO: function to generate the model
# TODO: add a flag to the ETL on whether to generate the model or simply not calculate fraud.

def read_model(model_path: str = DEFAULT_MODEL_PATH):
    return LRM.load(f'{model_path}/fraud_regression')

def predict_fraud(daily_user_transaction_df: DataFrame):

    # define important col names
    feature_vector = 'fraud_features'
    input_cols = ['total_value','avg_order_value'] 

    assembler = VectorAssembler(
        # which column to combine
        inputCols=input_cols, 
        # How should the combined columns be named
        outputCol=feature_vector
    )

    # apply the transforms
    daily_user_transaction_df = assembler.transform(daily_user_transaction_df)
    
    # get the lm and predict the data
    lm = read_model()
    daily_user_transaction_df = lm.transform(daily_user_transaction_df) \
        .withColumnRenamed('prediction', 'fraud_prob')

    return daily_user_transaction_df