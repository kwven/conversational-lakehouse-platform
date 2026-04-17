from pyspark.sql import SparkSession , functions

class SparkSessionFactory:
    @staticmethod
    def create(APPNAME:str):
        spark = (
            SparkSession.builder
            .appName(APPNAME)
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        return spark
