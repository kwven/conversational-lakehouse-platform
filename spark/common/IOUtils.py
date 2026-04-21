from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery

from common.Config import Config


class IOUtils:
    @staticmethod
    def read_stream(spark: SparkSession, topic: str):
        reader = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", Config.BOOTSTRAP_SERVERS)
            .option("subscribe", topic)
            .option("startingOffsets", Config.OFFSET)
        )

        if Config.MAX_OFFSETS_PER_TRIGGER:
            reader = reader.option("maxOffsetsPerTrigger", Config.MAX_OFFSETS_PER_TRIGGER)

        return reader.load()

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

    @staticmethod
    def read_delta_stream(spark,bronze_table):
        read = spark.readStream.format('delta').load(Config.BRONZE_PATH + bronze_table)
        return read
    @staticmethod
    def write_delta_stream(df:DataFrame,merge_to_silver,silver_path:str,path_table:str):
        target = silver_path + path_table
        df.writeStream.format("delta").foreachBatch(merge_to_silver).option("checkpointLocation", Config.CHECKPOINT_LOCATION + target).start()
