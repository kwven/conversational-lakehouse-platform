import os

from pyspark.sql import types as T


def _env(name: str, default=None):
    value = os.getenv(name)
    return value if value not in (None, "") else default


class Config:
    BOOTSTRAP_SERVERS = _env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    OFFSET = _env("STREAM_STARTING_OFFSETS", "earliest")
    MAX_OFFSETS_PER_TRIGGER = _env("STREAM_MAX_OFFSETS_PER_TRIGGER")
    CHECKPOINT_LOCATION = _env("CHECKPOINT_LOCATION", "s3a://checkpoints/")
    KAFKA_TOPIC = _env("KAFKA_TOPIC_PREFIX", "instacart.source.")
    BRONZE_PATH = _env("BRONZE_PATH", "s3a://lakehouse/")
    SILVER_PATH = _env("SILVER_PATH", "s3a://lakehouse/silver/")
    GOLD_PATH = _env("GOLD_PATH","s3a://lakehouse/gold/")
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
    
