''' Related to generating and using models for fraud prediction.
TODO: commenting
'''

from pyspark.sql import DataFrame
from pyspark.ml.regression import LinearRegression as LR
from pyspark.ml.regression import LinearRegressionModel as LRM
from pyspark.ml.feature import VectorAssembler as VA

DEFAULT_MODEL_PATH = './models' # where the raw data is
DEFAULT_FRAUD_FEATURE_COLNAME = 'fraud_features'
DEFAULT_FRAUD_MODEL_NAME = 'fraud_regression'

# TODO: function to generate the model
# TODO: add a flag to the ETL on whether to generate the model or simply not calculate fraud.

def read_model(model_path: str = DEFAULT_MODEL_PATH,
        model_name: str = DEFAULT_FRAUD_MODEL_NAME) -> LR:
    return LRM.load(f'{model_path}/{model_name}')

def predict_fraud(daily_user_transaction_df: DataFrame,
        model_path: str = DEFAULT_MODEL_PATH) -> DataFrame:

    # define important col names
    feature_vector = DEFAULT_FRAUD_FEATURE_COLNAME
    input_cols = ['tot_dollar_value','avg_dollar_value'] 

    assembler = VA(
        # which column to combine
        inputCols=input_cols, 
        # How should the combined columns be named
        outputCol=feature_vector
    )

    # apply the transforms
    daily_user_transaction_df = assembler.transform(daily_user_transaction_df)
    
    # get the lm and predict the data
    lm = read_model(DEFAULT_MODEL_PATH, DEFAULT_FRAUD_MODEL_NAME)
    daily_user_transaction_df = lm.transform(daily_user_transaction_df) \
        .withColumnRenamed('prediction', 'fraud_prob')

    return daily_user_transaction_df 

def generate_model(daily_user_transaction_df: DataFrame) -> LR:
    input_cols = ['tot_dollar_value', 'avg_dollar_value']

    # assemble the data into a vector format
    assembler = VA(
        # which column to combine
        inputCols=input_cols, 
        # How should the combined columns be named
        outputCol=DEFAULT_FRAUD_FEATURE_COLNAME
    )
    model_sdf = assembler.transform(daily_user_transaction_df.dropna('any'))

    return LR(
        featuresCol=DEFAULT_FRAUD_FEATURE_COLNAME, 
        labelCol='fraud_probability',
        maxIter=1000
    ).fit(model_sdf)


def write_model(model: LR, output_path: str = DEFAULT_MODEL_PATH,
    model_name: str = 'fraud_regression'):
    save_path = f'{output_path}/{model_name}'
    model.write().overwrite().save(save_path)