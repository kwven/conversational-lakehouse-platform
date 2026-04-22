from pyspark.sql import functions as F
from common.IOUtils import IOUtils
from common.Config import Config
from common.SparkSessionFactory import SparkSessionFactory
from pyspark.sql.window import Window

def extract(spark):
    products = IOUtils.read_delta(spark,"products_clean")
    orders = IOUtils.read_delta(spark,"orders_clean")
    order_products_prior = IOUtils.read_delta(spark,"order_products_prior_clean")
    return (products,orders,order_products_prior)
def transform(products,orders,order_products_prior):
    df_pr_or = order_products_prior.join(products,on ="product_id",how ="left").join(orders,on = "order_id",how ="left")
    df_clean = df_pr_or.groupBy("order_dow","order_hour_of_day").agg(F.countDistinct("order_id").alias("total_orders"),
                                                               F.count("product_id").alias("total_items"),F.countDistinct("product_id").alias("distinct_products")).withColumn("avg_items_per_order",F.col("total_items") / F.col("total_orders")
                                                               .withColumn("avg_distinct_products_per_order",F.col("distinct_products") / F.col("total_orders"))
                                                               ).withColumn("gold_created_at",F.current_timestamp())
    df_good = df_clean.select("order_dow","order_hour_of_day","total_orders","total_items","avg_items_per_order","avg_distinct_products_per_order","gold_created_at")
    return df_clean
def load(table):
    return IOUtils.write_delta(table,"average_basket_size_clean")
def main():
    spark = SparkSessionFactory.create("gold-average-basket-size")
    products,orders,order_products_prior = extract(spark)
    table = transform(products,orders,order_products_prior)
    load(table)
if __name__ == "__main__":
    main()