import os
import sys
import traceback
import json

import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import  ArrayType,StructType, StructField, BooleanType, StringType, IntegerType, DateType, FloatType,DoubleType, LongType
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, explode,col, expr,when,to_date, sum, from_json,size,length
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

def fix_json_format(crew_str):
    if crew_str is None:
        return None
    try:
        # Dùng json.loads để kiểm tra nếu hợp lệ, nếu không thì sửa
        fixed_json = json.dumps(eval(crew_str))  # Chuyển đổi thành JSON chuẩn
        return fixed_json
    except Exception as e:
        return None  # Trả về None nếu có lỗi
fix_json_udf = udf(fix_json_format, StringType())        

df = spark.read.format("delta").load("s3a://lakehouse/silver/movies")
df = df.withColumn("genres", fix_json_udf(col("genres")))

genres_schema = ArrayType(
    StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])
)
df_parsed = df.withColumn("genres", from_json(col("genres"), genres_schema))
df_exploded = df_parsed.withColumn("genres", explode(col("genres")))

df_selected_Dim = df_exploded.select(
    col("genres.id"),
    col("genres.name")
)
df_selected_Dim = df_selected_Dim.dropDuplicates(["id"])
df_selected_Bridge = df_exploded.select(
    col("genres.id").alias("genres_id"),
    col("id").cast("Integer")
)
df_selected_Bridge = df_selected_Bridge.dropDuplicates(["id", "genres_id"])
try:
    dim_genre = DeltaTable.forPath(spark, "s3a://lakehouse/gold/dim_genre")
    dim_genre.alias("target").merge(
        df_selected_Dim.alias("source"),
        "target.id = source.id"
    ).whenNotMatchedInsertAll().execute()
except:
    df_selected_Dim.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("s3a://lakehouse/gold/dim_genre")
try:
    movie_genre = DeltaTable.forPath(spark, "s3a://lakehouse/gold/movie_genre")
    movie_genre.alias("target").merge(
        df_selected_Bridge.alias("source"),
        "target.id = source.id AND target.genres_id = source.genres_id"
    ).whenNotMatchedInsertAll().execute()
except:
    df_selected_Bridge.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("s3a://lakehouse/gold/movie_genre")


