from pyspark import SparkSession , functions 
from Config import Config 
class IOUtils:
    def read_stream(self,spark:SparkSession,topic:str):
        df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers",Config.BOOTSTRAP_SERVERS)
            .option("subscribe",topic)
            .option("startingOffsets",Config.OFFSET)
            .load()
        )
        return df
    def write_stream(self,df:DataFrame,bronze_path:str,path_table:str):
        return(
            df.writeStream.format("delta")
            .option("outputMode","append")
            .option("checkpointLocation",Config.CHECKPOINT_LOCATION + path_table)
            .start(bronze_path + path_table)
        )  
