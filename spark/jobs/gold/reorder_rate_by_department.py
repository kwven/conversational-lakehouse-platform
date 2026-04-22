from pyspark.sql import functions as F
from common.IOUtils import IOUtils
from common.Config import Config
from common.SparkSessionFactory import SparkSessionFactory
from pyspark.sql.window import Window

def extract(spark):
    products = IOUtils.read_delta(spark,"products_clean")
    orders = IOUtils.read_delta(spark,"orders_clean")
    departments = IOUtils.read_delta(spark,"departments_clean")
    aisles = IOUtils.read_delta(spark,"aisles_clean")
    order_products_prior = IOUtils.read_delta(spark,"order_products_prior_clean")
    return (products,orders,departments,aisles,order_products_prior)
def transform(products,orders,departments,aisles,order_products_prior):
    df_pr_or = order_products_prior.join(products,on ="product_id",how ="left").join(orders,on = "order_id",how ="left")
    df_all = df_pr_or.join(departments,on = "department_id",how = "left").join(aisles,on = "aisle_id",how = "left")
    df_clean = df_all.groupBy("department_id","department").agg(F.count("*").alias("total_items"),
                                                               F.sum(F.col("reordered")).alias("total_reorders"),
                                                               F.round(F.avg(F.col("reordered") * 100),2).alias("reorder_rate_pct"),
                                                               F.countDistinct("product_id").alias("distinct_products"),                                                               
                                                               F.countDistinct("order_id").alias("distinct_orders"),
                                                               F.countDistinct("user_id").alias("unique_customers"),
                                                               ).withColumn(
                                                                    "department_rank",
                                                                    F.dense_rank().over(
                                                                        Window.orderBy(
                                                                            F.col("total_items").desc(),
                                                                            F.col("distinct_orders").desc(),
                                                                            F.col("total_reorders").desc()
                                                                        )
                                                                    )
                                                                ).withColumn("gold_created_at",F.current_timestamp())
    return df_clean
def load(table):
    return IOUtils.write_delta(table,"reorder_rate_by_department_clean")
def main():
    spark = SparkSessionFactory.create("gold-reorder-rate-by-department")
    products,orders,departments,aisles,order_products_prior = extract(spark)
    table = transform(products,orders,departments,aisles,order_products_prior)
    load(table)
if __name__ == "__main__":
    main()