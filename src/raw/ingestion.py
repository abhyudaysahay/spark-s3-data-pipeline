from utils.logger import get_logger

logger = get_logger(__name__)

def read_raw_data(spark, path):
    try:
        df = spark.read \
                .option('header', 'true') \
                .option('inferSchema', 'true') \
                .option('quote', '"') \
                .option('escape', '"') \
                .option('multiline', 'true') \
                .csv(path)

        logger.info('data ingested successfully')

        return df
    except Exception as e:
        logger.error(e)
    