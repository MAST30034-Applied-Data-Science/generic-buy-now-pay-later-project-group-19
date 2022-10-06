''' Related to generating and using models for fraud prediction.
TODO: commenting
'''

from pyspark.sql import DataFrame
from pyspark.ml.regression import LinearRegressionModel as LRM
from pyspark.ml.feature import VectorAssembler as VA

DEFAULT_MODEL_PATH = './models' # where the raw data is

def read_model(model_path: str = DEFAULT_MODEL_PATH):
    return LRM.load(f'{model_path}/fraud_regression')

def predict_fraud(compute_daily_user_transaction_df: DataFrame):

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
    daily_user_transactions_df = assembler.transform(daily_user_transaction_df)
    
    # get the lm and predict the data
    lm = read_model(model_path)
    daily_user_transaction_df = lm.transform(predict_sdf) \
        .withColumnRenamed('prediction', 'fraud_prob')

    return daily_user_transaction_df