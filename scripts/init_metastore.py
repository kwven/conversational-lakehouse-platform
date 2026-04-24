from common.SparkSessionFactory import SparkSessionFactory

spark = SparkSessionFactory.create("init-metastore")

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

print("Created databases: bronze, silver, gold")
