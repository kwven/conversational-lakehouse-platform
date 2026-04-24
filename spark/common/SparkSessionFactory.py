from pyspark.sql import SparkSession , functions

class SparkSessionFactory:
    @staticmethod
    def create(APPNAME:str):
        spark = (
            SparkSession.builder
            .appName(APPNAME)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
            .config("spark.hadoop.fs.s3a.access.key", "minio")
            .config("spark.hadoop.fs.s3a.secret.key", "minio123")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .enableHiveSupport()
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        return spark
