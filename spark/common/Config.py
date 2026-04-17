from pyspark.sql import types as T 
class Config:
    BOOTSTRAP_SERVERS = "kafka:9092"
    OFFSET = "earliest"
    CHECKPOINT_LOCATION = "s3a://lakehouse/checkpoints/"
    KAFKA_TOPIC = "instacart.source."
    BRONZE_PATH = "s3a://lakehouse/"
    SOURCE_SCHEMA = T.StructType([
        T.StructField("version",T.StringType(),True), 
        T.StructField("connector",T.StringType(),True),
        T.StructField("name",T.StringType(),True),
        T.StructField("ts_ms",T.StringType(),True),
        T.StructField("ts_us", T.LongType(), True),
        T.StructField("ts_ns", T.LongType(), True),
        T.StructField("snapshot",T.StringType(),True),
        T.StructField("db",T.StringType(),True),
        T.StructField("sequence", T.StringType(), True),
        T.StructField("schema",T.StringType(),True),
        T.StructField("table",T.StringType(),True),
        T.StructField("txId", T.LongType(), True),
        T.StructField("lsn", T.LongType(), True),
        T.StructField("xmin", T.LongType(), True),

    ]   
    )
    
