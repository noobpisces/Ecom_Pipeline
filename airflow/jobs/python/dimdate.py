import os
import sys
import traceback
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import  explode,col, expr,when,to_date, sum, from_json,size,length
from pyspark.sql.types import  ArrayType,StructType, StructField, BooleanType, StringType, IntegerType, DateType, FloatType,DoubleType, LongType
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_date, date_add, lit
import datetime
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



start_date = datetime.date(1800, 1, 1)
end_date = datetime.date(2200, 1, 1)

days_diff = (end_date - start_date).days
df_dates = spark.range(0, days_diff + 1).select(
    date_add(to_date(lit(str(start_date))), col("id").cast("int")).alias("date")
)

# 4. Trích xuất các trường tương tự như trước
df_dim_date = df_dates.select(
    F.col("date").alias("full_date"),
    F.dayofweek("date").alias("DayOfWeek"),
    F.date_format("date", "EEEE").alias("DayName"),
    F.dayofmonth("date").alias("DayOfMonth"),
    F.dayofyear("date").alias("DayOfYear"),
    F.weekofyear("date").alias("WeekOfYear"),
    F.date_format("date", "MMMM").alias("MonthName"),
    F.month("date").alias("MonthOfYear"),
    F.quarter("date").alias("Quarter"),
    F.year("date").alias("Year"),
    F.when((F.dayofweek("date") >= 2) & (F.dayofweek("date") <= 6), True).otherwise(False).alias("IsWeekDay")
).withColumn(
    "date_id",
    F.col("Year") * 10000 + F.col("MonthOfYear") * 100 + F.col("DayOfMonth")
)
df_dim_date = df_dim_date.dropDuplicates(["date_id"])
# 5. Ghi dữ liệu vào Delta Lake bảng dim_date
try:
    dimdate = DeltaTable.forPath(spark, "s3a://lakehouse/gold/dim_date")
    dimdate.alias("target").merge(
        df_dim_date.alias("source"),
        "target.date_id = source.date_id"
    ).whenNotMatchedInsertAll().execute()
except:
    df_dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("s3a://lakehouse/gold/dim_date")