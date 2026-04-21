from common.Config import Config
from common.IOUtils import IOUtils
from common.SparkSessionFactory import SparkSessionFactory
from pyspark.sql import DataFrame , functions as F , types as T
from typing import Callable
from delta.tables import DeltaTable
from pyspark.sql.window import Window
def extract(spark):
    read = IOUtils.read_delta_stream(spark,bronze_table="bronze/departments")
    return read

def transform(df:DataFrame,batch_id: int):
    if df.isEmpty():
        return 
    window = Window.partitionBy(F.col("department_id")).orderBy(F.col("_connector_ts_ms").desc_nulls_last(),F.col("ingested_at").desc_nulls_last())
    df_prepared = df.select(
        F.col("department_id").cast(T.IntegerType()).alias("department_id"),
        F.col("department").cast(T.StringType()).alias("department"),
        F.col("_op").cast("string").alias("_op"),
        F.col("_connector_ts_ms").cast("long").alias("_connector_ts_ms"),
        F.col("_ingested_at").cast("timestamp").alias("ingested_at"),
        ).filter(F.col("department_id").isNotNull()).withColumn("updated_at",F.to_timestamp(F.from_unixtime(F.col("_connector_ts_ms") / 1000.0))
        ).withColumn("is_deleted",F.when(F.col("_op") == "d",F.lit(True)).otherwise(F.lit(False)))
    
    df_clean = df_prepared.withColumn("flag",F.row_number().over(window)).filter(F.col("flag") == 1).drop("flag")

    spark = df_clean.sparkSession
    if not DeltaTable.isDeltaTable(spark,Config.SILVER_PATH + "departments_clean"):
        (df_clean.select(
                "department_id",
                "department",
                "updated_at",          
                "ingested_at",          
                "is_deleted"        
            )).write.format("delta").mode("overwrite").save(Config.SILVER_PATH + "departments_clean")
        return
    
    target = DeltaTable.forPath(spark, Config.SILVER_PATH + "departments_clean")

    (
        target.alias("t")
        .merge(
            df_clean.alias("c"),
            "t.department_id = c.department_id"
        )
        .whenMatchedUpdate(condition = "c.updated_at > t.updated_at",
            set={
            "department": "c.department",
            "updated_at": "c.updated_at",
            "ingested_at": "c.ingested_at",
            "is_deleted": "c.is_deleted",
        })
        .whenNotMatchedInsert(values={
            "department_id": "c.department_id",
            "department": "c.department",
            "updated_at": "c.updated_at",
            "ingested_at": "c.ingested_at",
            "is_deleted": "c.is_deleted",
        })
        .execute()
    )

def load(df_clean:DataFrame,merge_to_silver:Callable):
    return IOUtils.write_delta_stream(df_clean,merge_to_silver, Config.SILVER_PATH, "departments_clean")
    
def main():
    spark = SparkSessionFactory.create("silver-departments")
    df = extract(spark)
    load(df,transform)
if __name__ == "__main__":
    main()