#order_day_of_week order_hour,days_since_previous_order
from pyspark.sql import functions as F
from common.IOUtils import IOUtils
from common.Config import Config
from common.SparkSessionFactory import SparkSessionFactory
def extract(spark):
    read = IOUtils.read_delta(spark,"orders_clean")
    return read
def transform(df):
    return df.select(
        "order_id",
        "user_id",
        "eval_set",
        "order_number",
        "order_dow",
        "order_hour_of_day",
        "days_since_prior_order",
    ).groupBy(F.col("order_dow"),F.col("order_hour_of_day")).agg(F.countDistinct("order_id").alias("total_orders"),
                                                                F.countDistinct("user_id").alias("active_customer"),
                                                                F.avg("days_since_prior_order").alias("avg_days_since_previous_order")).withColumn("gold_created_at",F.current_timestamp())
    
def load(table):
    return IOUtils.write_delta(table,"order_demand_by_time")
def main():
    spark = SparkSessionFactory.create("gold-order-demand-by-time")
    df =extract(spark)
    table = transform(df)
    load(table)
if __name__ == "__main__":
    main()