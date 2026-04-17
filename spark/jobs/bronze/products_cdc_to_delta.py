from common.IOUtils import IOUtils
from common.Config import Config
from common.SparkSessionFactory import SparkSessionFactory
 
from pyspark.sql import SparkSession, DataFrame, functions as F, types as T
from pyspark.sql.streaming import StreamingQuery

topic = Config.KAFKA_TOPIC + "products"
row_schema = T.StructType([
    T.StructField("product_id",T.IntegerType(),True),
    T.StructField("product_name",T.StringType(),True),
    T.StructField("aisle_id",T.IntegerType(),True),
    T.StructField("department_id",T.IntegerType(),True),
])

source_schema = Config.SOURCE_SCHEMA

debezium_schema = T.StructType([
    T.StructField("before",row_schema,nullable=True),
    T.StructField("after",row_schema,nullable=True),
    T.StructField("source",source_schema),
    T.StructField("op",T.StringType(),True),
    T.StructField("ts_ms",T.StringType(),True),
    T.StructField("ts_us", T.LongType(), True),
    T.StructField("ts_ns", T.LongType(), True),
]
)
def extract_products(spark: SparkSession):
    df = IOUtils.read_stream(spark, topic)
    return df

def transform_products(df: DataFrame):
    df_parse = df.select(
        F.col("key").cast("string").alias("key_json"),
        F.col("value").cast("string").alias("value_json"),
        F.col("topic").alias("_topic"),
        F.col("partition").alias("_partition"),
        F.col("offset").alias("_offset"),
        F.col("timestamp").alias("_kafka_timestamp"),
    ).withColumn("payload",F.from_json(F.col("value_json"),debezium_schema))
    ## buisness columns
    df_transform= df_parse.select(
        F.coalesce(F.col("payload.after.product_id"),F.col("payload.before.product_id")).alias("product_id"),
        F.coalesce(F.col("payload.after.product_name"),F.col("payload.before.product_name")).alias("product_name"),
        F.coalesce(F.col("payload.after.aisle_id"),F.col("payload.before.aisle_id")).alias("aisle_id"),
        F.coalesce(F.col("payload.after.department_id"),F.col("payload.before.department_id")).alias("department_id"),
    # raw payload preservation 
        F.to_json(F.col("payload.before")).alias("before_json"),
        F.to_json(F.col("payload.after")).alias("after_json"),
    ## metadata cdc 
        F.col("payload.source.version").alias("_source_version"),
        F.col("payload.source.connector").alias("_source_connector"),
        F.col("payload.source.name").alias("_source_name"),
        F.col("payload.source.ts_ms").alias("_source_ts_ms"),
        F.col("payload.source.ts_us").alias("_source_ts_us"),
        F.col("payload.source.ts_ns").alias("_source_ts_ns"),
        F.col("payload.source.snapshot").alias("_source_snapshot"),
        F.col("payload.source.db").alias("_source_db"),
        F.col("payload.source.sequence").alias("_source_sequence"),
        F.col("payload.source.schema").alias("_source_schema"),
        F.col("payload.source.table").alias("_source_table"),
        F.col("payload.source.txId").alias("_source_txId"),
        F.col("payload.source.lsn").alias("_source_lsn"),
        F.col("payload.source.xmin").alias("_source_xmin"),
        F.col("payload.op").alias("_op"),
        F.col("payload.ts_ms").alias("_connector_ts_ms"),
        F.col("payload.ts_us").alias("_connector_ts_us"),
        F.col("payload.ts_ns").alias("_connector_ts_ns"),

        F.col("_kafka_timestamp"),
        F.col("_topic"),
        F.col("_partition"),
        F.col("_offset"),


        F.current_timestamp().alias("_ingested_at"),
    )
    return df_transform

def load_products(df_clean: DataFrame) -> StreamingQuery:
    return IOUtils.write_stream(df_clean, Config.BRONZE_PATH, "bronze/products")

def main():
    spark = SparkSessionFactory.create("bronze-products")
    df_clean = transform_products(extract_products(spark))
    query = load_products(df_clean)
    query.awaitTermination()
if __name__ == "__main__":
    main()
