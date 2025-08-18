import os
import sys
import traceback
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode,col, expr,when,to_date, sum, from_json,size,length
from pyspark.sql.types import  ArrayType,StructType, StructField, BooleanType, StringType, IntegerType, DateType, FloatType,DoubleType, LongType
import json
from pyspark.sql import functions as F
from delta.tables import DeltaTable
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
df_dim_cast = spark.read.format("delta").load("s3a://lakehouse/silver/credit")
df_dim_cast= df_dim_cast.withColumn("cast", fix_json_udf(col("cast")))


cast_schema = ArrayType(
    StructType([
        StructField("cast_id", IntegerType(), True),
        StructField("character", StringType(), True),
        StructField("credit_id", StringType(), True),
        StructField("gender", IntegerType(), True),
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("order", IntegerType(), True),
        StructField("profile_path", StringType(), True)
    ])
)
df_parsed = df_dim_cast.withColumn("cast", from_json(col("cast"), cast_schema))

# Explode cột cast để có nhiều dòng
df_exploded = df_parsed.withColumn("cast", explode(col("cast")))

# Chọn các trường cần thiết
df_selected_Dim = df_exploded.select(
    col("cast.name"),
    col("cast.gender"),
    col("cast.profile_path"),
    col("cast.id")
)
df_selected_Dim = df_selected_Dim.dropDuplicates(["id"])

df_selected_Bridge = df_exploded.select(
    col("cast.id").alias("cast_id"),
    col("cast.character"),
    col("id").alias("movie_id"),
    col("cast.order")

)
df_selected_Bridge = df_selected_Bridge.dropDuplicates(["movie_id", "cast_id","order"])

print("df_exploded" ,df_exploded.count())
try:
    dim_cast = DeltaTable.forPath(spark, "s3a://lakehouse/gold/dim_cast")
    dim_cast.alias("target").merge(
        df_selected_Dim.alias("source"),
        "target.id = source.id"
    ).whenNotMatchedInsertAll().execute()
except:
    df_selected_Dim.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("s3a://lakehouse/gold/dim_cast")
    
try:
    movie_cast = DeltaTable.forPath(spark, "s3a://lakehouse/gold/movie_cast")
    movie_cast.alias("target").merge(
        df_selected_Bridge.alias("source"),
        "target.movie_id = source.movie_id AND target.cast_id = source.cast_id AND target.character = source.character"
    ).whenNotMatchedInsertAll().execute()
except:
    df_selected_Bridge.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("s3a://lakehouse/gold/movie_cast")