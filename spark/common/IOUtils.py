from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery

from common.Config import Config


class IOUtils:
    @staticmethod
    def read_stream(spark: SparkSession, topic: str):
        df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", Config.BOOTSTRAP_SERVERS)
            .option("subscribe", topic)
            .option("startingOffsets", Config.OFFSET)
            .load()
        )
        return df

    @staticmethod
    def write_stream(df: DataFrame, bronze_path: str, path_table: str) -> StreamingQuery:
        checkpoint_path = Config.CHECKPOINT_LOCATION + path_table
        target_path = bronze_path + path_table
        query = (
            df.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .start(target_path)
        )
        return query
