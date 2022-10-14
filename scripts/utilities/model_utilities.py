''' Related to generating and using models for fraud prediction.
'''

from pyspark.sql import DataFrame
from pyspark.ml.regression import LinearRegression as LR
from pyspark.ml.regression import LinearRegressionModel as LRM
from pyspark.ml.feature import VectorAssembler as VA

DEFAULT_MODEL_PATH = './models' # where the raw data is
DEFAULT_FRAUD_FEATURE_COLNAME = 'fraud_features'
DEFAULT_FRAUD_MODEL_NAME = 'fraud_regression'

def read_model(model_path: str = DEFAULT_MODEL_PATH,
        model_name: str = DEFAULT_FRAUD_MODEL_NAME) -> LR:
    """ Read a linear model.

    Args:
        model_path (str, optional): Model folder path. Defaults to `./models`.
        model_name (str, optional): Name of the model to read. Defaults to 'fraud_regression'.

    Returns:
        `LinearRegression`: The linear regression.
    """

    return LRM.load(f'{model_path}/{model_name}')


def predict_fraud(daily_user_transaction_df: DataFrame,
        model_path: str = DEFAULT_MODEL_PATH) -> DataFrame:
    """ Predict the fraud for a daily user transaction dataset.

    Args:
        daily_user_transaction_df (`DataFrame`): As the name implies.
        model_path (str, optional): Model folder path. Defaults to `./models`.

    Returns:
        `DataFrame`: The `daily_user_transaction_df` with fraud_provavilities assigned.
    """

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


def generate_fraud_model(daily_user_transaction_df: DataFrame) -> LR:
    """ Generate the fraud linear regression.

    Args:
        daily_user_transaction_df (`DataFrame`): As the name implies.

    Returns:
        `LinearRegression`: The linear regression.
    """
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
    """_summary_

    Args:
        model (LR): _description_
        output_path (str, optional): _description_. Defaults to DEFAULT_MODEL_PATH.
        model_name (str, optional): _description_. Defaults to 'fraud_regression'.
    """

    save_path = f'{output_path}/{model_name}'
    model.write().overwrite().save(save_path)