from pyspark.sql.functions import sum, year, month

def sales_by_region(df):
    return df.groupBy('region') \
                .agg(sum('sales').alias('total_sales'))

def sales_by_category(df):
    return df.groupBy('category', 'sub_category') \
                .agg(sum('sales').alias('total_sales'))

def monthly_sales(df):
    df_time = df \
                .withColumn('year', year('order_date')) \
                .withColumn('month', month('order_date'))

    return df_time.groupBy('year', 'month') \
                .agg(sum('sales').alias('monthly_sales')) \
                .orderBy('year', 'month')

def top_products(df):
    return df.groupBy('product_id', 'product_name') \
                .agg(sum('sales').alias('total_sales')) \
                .orderBy('total_sales', ascending=False)

def top_customers(df):
    return df.groupBy('customer_id', 'customer_name') \
                .agg(sum('sales').alias('total_sales')) \
                .orderBy('total_sales', ascending=False)
