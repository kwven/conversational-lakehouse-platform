from common.Config import Config
from common.IOUtils import IOUtils
from common.SparkSessionFactory import SparkSessionFactory
from pyspark.sql import DataFrame , functions as F , types as T
from typing import Callable
from delta.tables import DeltaTable
from pyspark.sql.window import Window
def extract(spark):
    read = IOUtils.read_delta_stream(spark,bronze_table="bronze/aisles")
    return read

def transform(df:DataFrame,batch_id: int):
    if df.isEmpty():
        return 
    window = Window.partitionBy(F.col("aisle_id")).orderBy(F.col("_connector_ts_ms").desc_nulls_last(),F.col("ingested_at").desc_nulls_last())
    df_prepared = df.select(
        F.col("aisle_id").cast(T.IntegerType()).alias("aisle_id"),
        F.col("aisle").cast(T.StringType()).alias("aisle"),
        F.col("_op").cast("string").alias("_op"),
        F.col("_connector_ts_ms").cast("long").alias("_connector_ts_ms"),
        F.col("_ingested_at").cast("timestamp").alias("ingested_at"),
        ).filter(F.col("aisle_id").isNotNull()).withColumn("updated_at",F.to_timestamp(F.from_unixtime(F.col("_connector_ts_ms") / 1000.0))
        ).withColumn("is_deleted",F.when(F.col("_op") == "d",F.lit(True)).otherwise(F.lit(False)))
    
    df_clean = df_prepared.withColumn("flag",F.row_number().over(window)).filter(F.col("flag") == 1).drop("flag")

    spark = df_clean.sparkSession
    if not DeltaTable.isDeltaTable(spark,Config.SILVER_PATH + "aisles_clean"):
        (df_clean.select(
                "aisle_id",
                "aisle",
                "updated_at",          
                "ingested_at",          
                "is_deleted"        
            )).write.format("delta").mode("overwrite").save(Config.SILVER_PATH + "aisles_clean")
        return
    
    target = DeltaTable.forPath(spark, Config.SILVER_PATH + "aisles_clean")

    (
        target.alias("t")
        .merge(
            df_clean.alias("c"),
            "t.aisle_id = c.aisle_id"
        )
        .whenMatchedUpdate(condition = "c.updated_at > t.updated_at",
            set={
            "aisle": "c.aisle",
            "updated_at": "c.updated_at",
            "ingested_at": "c.ingested_at",
            "is_deleted": "c.is_deleted",
        })
        .whenNotMatchedInsert(values={
            "aisle_id": "c.aisle_id",
            "aisle": "c.aisle",
            "updated_at": "c.updated_at",
            "ingested_at": "c.ingested_at",
            "is_deleted": "c.is_deleted",
        })
        .execute()
    )

def load(df_clean:DataFrame,merge_to_silver:Callable):
    return IOUtils.write_delta_stream(df_clean,merge_to_silver, Config.SILVER_PATH, "aisles_clean")
    
def main():
    spark = SparkSessionFactory.create("silver-aisles")
    df = extract(spark)
    load(df,transform)
if __name__ == "__main__":
    main()