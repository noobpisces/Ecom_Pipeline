import os
import sys
import traceback
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import  explode,col, expr,when,to_date, sum, from_json,size,length, udf
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
df_dim_crew = spark.read.format("delta").load("s3a://lakehouse/silver/credit")
df_dim_crew = df_dim_crew.withColumn("crew", fix_json_udf(col("crew")))
crew_schema = ArrayType(
    StructType([
        StructField("credit_id", StringType(), True),
        StructField("department", StringType(), True),
        StructField("gender", IntegerType(), True),
        StructField("id", IntegerType(), True),
        StructField("job", StringType(), True),
        StructField("name", StringType(), True),
        StructField("profile_path", StringType(), True)
    ])
)
df_parsed = df_dim_crew.withColumn("crew", from_json(col("crew"), crew_schema))

df_exploded = df_parsed.withColumn("crew", explode(col("crew")))

# Chọn các trường cần thiết
df_selected_Dim = df_exploded.select(
    col("crew.name"),
    col("crew.gender"),
    col("crew.id")
)

df_selected_Dim = df_selected_Dim.dropDuplicates(["id"])

df_selected_Bridge = df_exploded.select(
    col("crew.id").alias("crew_id"),
    col("crew.job"),
    col("crew.department"),
    col("id").alias("movie_id")
)
df_selected_Bridge = df_selected_Bridge.dropDuplicates(["movie_id", "crew_id"])


try:
    dim_crew = DeltaTable.forPath(spark, "s3a://lakehouse/gold/dim_crew")
    dim_crew.alias("target").merge(
        df_selected_Dim.alias("source"),
        "target.id = source.id"
    ).whenNotMatchedInsertAll().execute()
except:
    df_selected_Dim.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("s3a://lakehouse/gold/dim_crew")
    
try:
    movie_crew = DeltaTable.forPath(spark, "s3a://lakehouse/gold/movie_crew")
    movie_crew.alias("target").merge(
        df_selected_Bridge.alias("source"),
        "target.movie_id = source.movie_id AND target.crew_id = source.crew_id AND target.job = target.job AND target.department = target.department"
    ).whenNotMatchedInsertAll().execute()
except:
    df_selected_Bridge.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("s3a://lakehouse/gold/movie_crew")
