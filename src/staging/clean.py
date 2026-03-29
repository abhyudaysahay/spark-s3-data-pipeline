import re
from pyspark.sql.functions import col, to_date

def clean_columns(df):
        new_cols = []

        for c in df.columns:
            c = c.strip()
            c = c.lower()
            c = re.sub(r"[^\w]+", "_", c)       # replace special characters with _
            c = re.sub(r"_+", "_", c)           # remove multiple underscores
            c = c.strip('_')
            new_cols.append(c)

        return df.toDF(*new_cols)


def clean_data(df):
    
    # changing column name
    df_column_remnamed = clean_columns(df)

    # changing column datatypes
    df_clean = df_column_remnamed \
        .withColumn('order_date', to_date(col('order_date'), 'dd/MM/yyyy')) \
        .withColumn('ship_date', to_date(col('ship_date'), 'dd/MM/yyyy')) \
        .withColumn('postal_code', col('postal_code').cast('string'))

    return df_clean

