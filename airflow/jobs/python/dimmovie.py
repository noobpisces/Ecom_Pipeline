import os
import sys
import traceback
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import  explode,col, expr,when,to_date, sum, from_json,size,length
from pyspark.sql.types import  ArrayType,StructType, StructField, BooleanType, StringType, IntegerType, DateType, FloatType,DoubleType, LongType
from delta.tables import DeltaTable
from pyspark.sql import functions as F
import json

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

df = spark.read.format("delta").load("s3a://lakehouse/silver/movies")
df = df.select(
    col("id").cast("integer"),  
    "title", "original_title", col("spoken_languages").alias("language"), "overview", 
    "runtime", "tagline", "status", "homepage"
)
df = df.dropDuplicates(["id"])


try:
    dimmovie = DeltaTable.forPath(spark, "s3a://lakehouse/gold/dim_movie")
    dimmovie.alias("target").merge(
        df.alias("source"),
        "target.id = source.id"
    ).whenNotMatchedInsertAll().execute()
except:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("s3a://lakehouse/gold/dim_movie")