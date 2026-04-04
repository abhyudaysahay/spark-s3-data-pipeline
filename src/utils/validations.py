from utils.logger import get_logger

logger = get_logger(__name__)

def validate_data(df):
    logger.info("Validating data...")

    #checking empty dataframe
    if df.count() == 0:
        raise ValueError("DataFrame is empty")

    # Checking nulls in sales column
    null_sales = df.filter(df.sales.isNull()).count()
    if null_sales > 0:
        raise ValueError(f"Sales column has {null_sales} null values")

    
    logger.info("Data validation passed successfully")