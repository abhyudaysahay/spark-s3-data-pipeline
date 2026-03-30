from pyspark.sql import SparkSession
from utils.config_loader import load_config
from raw.ingestion import *
from staging.clean import *
from curated.transform import *
from utils.s3_writer import *
from utils.logger import get_logger


def main():
    spark = SparkSession.builder \
        .appName("ReadS3Data") \
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ) \
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        ) \
        .getOrCreate()

    logger = get_logger(__name__)

    logger.info('starting the pipeline......')


    try:

        try:
            config = load_config()
            logger.info('config loaded successfully....')
        except Exception as e:
            logger.error(e)

    # importing raw data
        logger.info('reading data....')
        df_raw = read_raw_data(spark, config['paths']['raw'])
        logger.info('file read successfully')

    # cleaning data
        logger.info('cleaning data....')
        df_clean = clean_data(df_raw)
        logger.info('data cleaned successfully')

    # loading data to staging layer
        logger.info('loading data to staging lyer......')
        write_parquet(df_clean, config['paths']['staging'], 'overwrite', ['region'])

    # analysing cleaned data
        logger.info('analyzing data........')
        sales_by_region_df = sales_by_region(df_clean)
        sales_by_category_df = sales_by_category(df_clean)
        monthly_sales_df = monthly_sales(df_clean)
        top_products_df = top_products(df_clean)
        top_customers_df = top_customers(df_clean)
        logger.info('data analysis completed')

    # loding analized data to s3
        logger.info('loading data to curated layer......')
        write_parquet(sales_by_region_df, config['paths']['curated']['sales_by_region'], 'overwrite', None)
        write_parquet(sales_by_category_df, config['paths']['curated']['sales_by_category'], 'overwrite', None)
        write_parquet(monthly_sales_df, config['paths']['curated']['monthly_sales'], 'overwrite', ['year'])
        write_parquet(top_products_df, config['paths']['curated']['top_products'], 'overwrite', None)
        write_parquet(top_customers_df, config['paths']['curated']['top_customers'], 'overwrite', None)

    finally:
        spark.stop()


if __name__ == '__main__':
    main()

