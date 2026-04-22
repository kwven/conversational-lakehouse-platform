from common.Config import Config
from common.IOUtils import IOUtils
from common.SparkSessionFactory import SparkSessionFactory
from pyspark.sql import DataFrame , functions as F , types as T
from typing import Callable
from delta.tables import DeltaTable
from pyspark.sql.window import Window
def extract(spark):
    read = IOUtils.read_delta_stream(spark,bronze_table="bronze/orders")
    return read

def transform(df:DataFrame,batch_id: int):
    if df.isEmpty():
        return 
    window = Window.partitionBy(F.col("order_id")).orderBy(F.col("_connector_ts_ms").desc_nulls_last(),F.col("ingested_at").desc_nulls_last())
    df_prepared = df.select(
        F.col("order_id").cast(T.IntegerType()).alias("order_id"),
        F.col("user_id").cast(T.IntegerType()).alias("user_id"),
        F.col("eval_set").cast(T.StringType()).alias("eval_set"),
        F.col("order_number").cast(T.IntegerType()).alias("order_number"),
        F.col("order_dow").cast(T.IntegerType()).alias("order_dow"),
        F.col("order_hour_of_day").cast(T.IntegerType()).alias("order_hour_of_day"),
        F.col("days_since_prior_order").cast(T.DecimalType(10,0)).alias("days_since_prior_order"),
        F.col("_op").cast("string").alias("_op"),
        F.col("_connector_ts_ms").cast("long").alias("_connector_ts_ms"),
        F.col("_ingested_at").cast("timestamp").alias("ingested_at"),
        ).filter(F.col("order_id").isNotNull()).withColumn("updated_at",F.to_timestamp(F.from_unixtime(F.col("_connector_ts_ms") / 1000.0))
        ).withColumn("is_deleted",F.when(F.col("_op") == "d",F.lit(True)).otherwise(F.lit(False)))
    
    df_clean = df_prepared.withColumn("flag",F.row_number().over(window)).filter(F.col("flag") == 1).drop("flag")

    spark = df_clean.sparkSession
    if not DeltaTable.isDeltaTable(spark,Config.SILVER_PATH + "orders_clean"):
        (df_clean.select(
                "order_id",
                "user_id",
                "eval_set",
                "order_number",
                "order_dow",
                "order_hour_of_day",       
                "days_since_prior_order",
                "updated_at",          
                "ingested_at",          
                "is_deleted"        
            )).write.format("delta").mode("overwrite").save(Config.SILVER_PATH + "orders_clean")
        return
    
    target = DeltaTable.forPath(spark, Config.SILVER_PATH + "orders_clean")

    (
        target.alias("t")
        .merge(
            df_clean.alias("c"),
            "t.order_id = c.order_id"
        )
        .whenMatchedUpdate(condition = "c.updated_at > t.updated_at",
            set={
            "user_id": "c.user_id",
            "eval_set": "c.eval_set",
            "order_number": "c.order_number",
            "order_dow": "c.order_dow",
            "order_hour_of_day": "c.order_hour_of_day",
            "days_since_prior_order": "c.days_since_prior_order",
            "updated_at": "c.updated_at",
            "ingested_at": "c.ingested_at",
            "is_deleted": "c.is_deleted",
        })
        .whenNotMatchedInsert(values={
            "order_id": "c.order_id",
            "user_id": "c.user_id",
            "eval_set": "c.eval_set",
            "order_number": "c.order_number",
            "order_dow": "c.order_dow",
            "order_hour_of_day": "c.order_hour_of_day",
            "days_since_prior_order": "c.days_since_prior_order",
            "updated_at": "c.updated_at",
            "ingested_at": "c.ingested_at",
            "is_deleted": "c.is_deleted",
        })
        .execute()
    )

def load(df_clean:DataFrame,merge_to_silver:Callable):
    return IOUtils.write_delta_stream(df_clean,merge_to_silver, "orders_clean")
    
def main():
    spark = SparkSessionFactory.create("silver-orders")
    df = extract(spark)
    load(df,transform)
if __name__ == "__main__":
    main()