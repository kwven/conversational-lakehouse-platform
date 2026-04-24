from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery

from common.Config import Config


class IOUtils:
    # beonze jobs
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
            .option("path", target_path)
            .option("checkpointLocation", checkpoint_path)
            .queryName("bronze-"+path_table+"-cdc")
            .toTable("bronze."+path_table)
        )
        return query
    # silver jobs 
    @staticmethod
    def read_delta_stream(spark,bronze_table):
        read = spark.readStream.format('delta').load(Config.BRONZE_PATH + bronze_table)
        return read
    @staticmethod
    def write_delta_stream(df:DataFrame,merge_to_silver,path_table:str):
        target = "silver/" + path_table
        df.writeStream.format("delta").foreachBatch(merge_to_silver).option("checkpointLocation", Config.CHECKPOINT_LOCATION + target).queryName("silver-"+path_table+"-clean").start()
    # gold jobs
    @staticmethod
    def read_delta(spark,silver_table):
        read = spark.read.format("delta").load(Config.SILVER_PATH + silver_table)
        return read
    def write_delta(df:DataFrame,gold_table):
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option("path",Config.GOLD_PATH + gold_table).saveAsTable("gold."+gold_table)

