def read_raw_data(spark, path):
    try:
        df = spark.read \
                .option('header', 'true') \
                .option('inferSchema', 'true') \
                .option('quote', '"') \
                .option('escape', '"') \
                .option('multiline', 'true') \
                .csv(path)

        print(f'data ingested successfully')

        return df
    except Exception as e:
        print(f'Error occured: {e}')
    