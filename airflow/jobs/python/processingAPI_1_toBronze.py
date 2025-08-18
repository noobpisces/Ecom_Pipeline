from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, DateType, BooleanType
from pyspark.sql.functions import (
    col, from_json, explode, to_date, date_format,
    dayofweek, dayofmonth, dayofyear, weekofyear,
    month, quarter, year, when, unix_timestamp,current_timestamp
)



# Tạo Spark Session
spark = SparkSession.builder \
    .appName("Kafka Streaming") \
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
    .config("spark.sql.warehouse.dir", "s3a://lakehouse/") \
    .getOrCreate()


import time
last_data_time = time.time()
timeout_seconds = 240
def process_batch(df, batch_id):
    global last_data_time
    if not df.rdd.isEmpty():
        last_data_time = time.time()
    else:
        print(f"Batch {batch_id} is empty.")
def monitor_timeout():
    while True:
        if time.time() - last_data_time > timeout_seconds:
            print(f"No data for {timeout_seconds} seconds. Stopping Spark streaming...")
            for q in spark.streams.active:
                q.stop()
            break
        time.sleep(5)
import threading
threading.Thread(target=monitor_timeout, daemon=True).start()
# Định nghĩa schema cho dữ liệu JSON (runtime được định nghĩa là DoubleType)
schema_movie = StructType([
    StructField("id", IntegerType(), True),
    StructField("original_title", StringType(), True),
    StructField("title", StringType(), True),
    StructField("original_language", StringType(), True),
    StructField("overview", StringType(), True),
    StructField("runtime", DoubleType(), True),
    StructField("tagline", StringType(), True),
    StructField("status", StringType(), True),
    StructField("homepage", StringType(), True),
    StructField("budget", IntegerType(), True),
    StructField("popularity", DoubleType(), True),
    StructField("revenue", IntegerType(), True),
    StructField("vote_average", DoubleType(), True),
    StructField("vote_count", IntegerType(), True),
    StructField("release_date", StringType(), True),
    StructField("genres", ArrayType(StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])), True)
])
schema_crew = StructType([
    StructField("id", IntegerType(), True),
    StructField("cast", ArrayType(
        StructType([
            StructField("adult", BooleanType(), True),
            StructField("gender", IntegerType(), True),
            StructField("id", IntegerType(), True),
            StructField("known_for_department", StringType(), True),
            StructField("name", StringType(), True),
            StructField("original_name", StringType(), True),
            StructField("popularity", DoubleType(), True),
            StructField("profile_path", StringType(), True),
            StructField("cast_id", IntegerType(), True),
            StructField("character", StringType(), True),
            StructField("credit_id", StringType(), True),
            StructField("order", IntegerType(), True)
        ])
    ), True),
    StructField("crew", ArrayType(
        StructType([
            StructField("adult", BooleanType(), True),
            StructField("gender", IntegerType(), True),
            StructField("id", IntegerType(), True),
            StructField("known_for_department", StringType(), True),
            StructField("name", StringType(), True),
            StructField("original_name", StringType(), True),
            StructField("popularity", DoubleType(), True),
            StructField("profile_path", StringType(), True),
            StructField("credit_id", StringType(), True),
            StructField("department", StringType(), True),
            StructField("job", StringType(), True)
        ])
    ), True)
])
schema_keyword = StructType([
    StructField("id", IntegerType(), True),
    StructField("keywords", ArrayType(
        StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])
    ), True)
])
# Đọc dữ liệu từ Kafka
# kafka_df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "kafka:9092") \
#     .option("subscribe", "tmdb_movies") \
#     .option("startingOffsets", "latest") \
#     .load()
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "tmdb_movies,tmdb_crews,tmdb_keywords") \
    .option("startingOffsets", "earliest") \
    .load()
# Chuyển đổi dữ liệu từ binary sang string và parse JSON
# json_df = kafka_df.selectExpr("CAST(value AS STRING)")
# parsed_df = json_df.withColumn("data", from_json(col("value"), schema)).select("data.*")

# json_df = kafka_df.selectExpr("CAST(value AS STRING) as value", "topic")
# movies_df = json_df.filter(col("topic") == "tmdb_movies") \
#                    .withColumn("data", from_json(col("value"), schema_movie)) \
#                    .select("data.*") \
#                    .withColumn("release_date", to_date(col("release_date"), "yyyy-MM-dd")) \
#                    .withColumn("read_time", current_timestamp())

# crews_df = json_df.filter(col("topic") == "tmdb_crews") \
#                   .withColumn("data", from_json(col("value"), schema_crew)) \
#                   .select("data.*") \
#                   .withColumn("read_time", current_timestamp())

# keywords_df = json_df.filter(col("topic") == "tmdb_keywords") \
#                      .withColumn("data", from_json(col("value"), schema_keyword)) \
#                      .select("data.*") \
#                      .withColumn("read_time", current_timestamp())

# movies_bronze = movies_df.writeStream \
#     .foreachBatch(process_batch) \
#     .format("delta") \
#     .outputMode("append") \
#     .option("checkpointLocation", "s3a://lakehouse/check/Bronze_Movies") \
#     .option("path", "s3a://lakehouse/bronze/Bronze_Movies_API") \
#     .start()

# crews_bronze = crews_df.writeStream \
#     .format("delta") \
#     .outputMode("append") \
#     .option("checkpointLocation", "s3a://lakehouse/check/Bronze_Crews") \
#     .option("path", "s3a://lakehouse/bronze/Bronze_Crews_API") \
#     .start()

# keywords_bronze = keywords_df.writeStream \
#     .format("delta") \
#     .outputMode("append") \
#     .option("checkpointLocation", "s3a://lakehouse/check/Bronze_Keywords") \
#     .option("path", "s3a://lakehouse/bronze/Bronze_Keywords_API") \
#     .start()

json_df = kafka_df.selectExpr("CAST(value AS STRING) as raw_json", "topic") \
                  .withColumn("read_time", current_timestamp())
movies_df = json_df.filter(col("topic") == "tmdb_movies")
crews_df = json_df.filter(col("topic") == "tmdb_crews")
keywords_df = json_df.filter(col("topic") == "tmdb_keywords")
movies_bronze = movies_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://lakehouse/check/Bronze_Movies") \
    .option("path", "s3a://lakehouse/bronze/Bronze_Movies_API") \
    .start()
crews_bronze = crews_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://lakehouse/check/Bronze_Crews") \
    .option("path", "s3a://lakehouse/bronze/Bronze_Crews_API") \
    .start()
keywords_bronze = keywords_df.writeStream \
    .foreachBatch(process_batch) \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://lakehouse/check/Bronze_Keywords") \
    .option("path", "s3a://lakehouse/bronze/Bronze_Keywords_API") \
    .start()

spark.streams.awaitAnyTermination()