import os
import sys
import traceback
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import  explode,col, expr,when,to_date, sum, from_json,size,length
from pyspark.sql.types import  ArrayType,StructType, StructField, BooleanType, StringType, IntegerType, DateType, FloatType,DoubleType, LongType
from delta.tables import DeltaTable
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("MinIO with Delta Lake") \
    .master('spark://spark-master:7077') \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "conbo123") \
    .config("spark.hadoop.fs.s3a.secret.key", "123conbo") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
    .config("delta.enable-non-concurrent-writes", "true") \
    .config('spark.sql.warehouse.dir', "s3a://lakehouse/") \
    .getOrCreate()


df = spark.read.format("parquet").load("s3a://lakehouse/silver/movies")
df_fact = df.withColumn(
    "date_id",
    (F.year("release_date") * 10000 + F.month("release_date") * 100 + F.dayofmonth("release_date")).cast("long")
)

df_fact = df_fact.select(
    col("id").cast("integer"),
    col("budget").cast("integer"),
    col("popularity").cast("double"),
    col("revenue"),
    col("vote_average"),
    col("vote_count"),
    col("date_id")
)

df_fact= df_fact.dropDuplicates(["id"])

try:

    fact_movie = DeltaTable.forPath(spark, "s3a://lakehouse/gold/fact_movies")

    fact_movie.alias("target").merge(
        df_fact.alias("source"),
        "target.id = source.id"
    ).whenNotMatchedInsertAll().execute()
except :
    df_fact.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("s3a://lakehouse/gold/fact_movies")
