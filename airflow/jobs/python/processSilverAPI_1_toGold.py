import os
import sys
import traceback
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import  explode,col, expr,when,to_date, sum, from_json,size,length,  lit, to_timestamp
from pyspark.sql.types import  ArrayType,StructType, StructField, BooleanType, StringType, IntegerType, DateType, FloatType,DoubleType, LongType
from pyspark.sql.functions import (
    col, from_json, explode, to_date, date_format,
    dayofweek, dayofmonth, dayofyear, weekofyear,
    month, quarter, year, when, unix_timestamp,current_timestamp, max
)
from delta.tables import DeltaTable
from airflow.models import Variable
from datetime import datetime, timedelta
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

df_Movies = spark.read.format("delta").load("s3a://lakehouse/silver/Silver_Movies_API")
df_Crews = spark.read.format("delta").load("s3a://lakehouse/silver/Silver_Crews_API")
df_Keywords = spark.read.format("delta").load("s3a://lakehouse/silver/Silver_Keywords_API")

# last_read_time = Variable.get("last_read_time_SG", default_var="1970-01-01T00:00:00")
# last_read_time_ts = to_timestamp(lit(last_read_time), "yyyy-MM-dd HH:mm:ss")
try:
    readTime = spark.read.format("delta").load("s3a://lakehouse/ReadTime")
except:
    spark.sql("""
CREATE TABLE IF NOT EXISTS delta.`s3a://lakehouse/ReadTime` (
    task_id STRING,
    last_read_time TIMESTAMP
) USING DELTA
""")
    spark.sql("""
    INSERT INTO delta.`s3a://lakehouse/ReadTime`
    VALUES 
      ('BatchApi_Process_Movies', '1970-01-01 00:00:00'),
      ('BatchApi_Process_Crews', '1970-01-01 00:00:00'),
      ('BatchApi_Process_Keywords', '1970-01-01 00:00:00')
""")
    readTime = spark.read.format("delta").load("s3a://lakehouse/ReadTime")

result_movie = readTime.filter(f"task_id = 'BatchApi_Process_Movies'").select("last_read_time").collect()
result_crew = readTime.filter(f"task_id = 'BatchApi_Process_Crews'").select("last_read_time").collect()
result_keyword = readTime.filter(f"task_id = 'BatchApi_Process_Keywords'").select("last_read_time").collect()

last_read_time_movie = result_movie[0][0]
last_read_time_crew = result_crew[0][0]
last_read_time_keyword = result_keyword[0][0]


df_Movies = df_Movies.filter(f"read_time > '{last_read_time_movie}'")
df_Crews = df_Crews.filter(f"read_time > '{last_read_time_crew}'")
df_Keywords = df_Keywords.filter(f"read_time > '{last_read_time_keyword}'")

# --------------------------------------------------
# FACT TABLE: fact_movie
# --------------------------------------------------
fact_movie_df = df_Movies.select(
    col("id"),
    col("budget"),
    col("popularity"),
    col("revenue"),
    col("vote_average"),
    col("vote_count"),
    col("date_id")
).dropDuplicates(["id"])

try:

    fact_movie = DeltaTable.forPath(spark, "s3a://lakehouse/gold/fact_movies")

    fact_movie.alias("target").merge(
        fact_movie_df.alias("source"),
        "target.id = source.id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
except :
    fact_movie_df.write.format("delta").save("s3a://lakehouse/gold/fact_movies")




# --------------------------------------------------
# DIMENSION TABLE: dim_movie
# Lưu ý: Đổi tên các cột để khớp với schema của Delta table hiện có:
# Schema mong đợi: id, title, original_title, language, overview, runtime, tagline, status, homepage
# --------------------------------------------------
dimmovie_df = df_Movies.select(
    col("id"), 
    col("title"),
    col("original_title"), 
    col("language"), 
    col("overview"),
    col("runtime"),
    col("tagline"),
    col("status"),
    col("homepage") 
).dropDuplicates(["id"])

try:
    dimmovie = DeltaTable.forPath(spark, "s3a://lakehouse/gold/dim_movie")
    dimmovie.alias("target").merge(
        dimmovie_df.alias("source"),
        "target.id = source.id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
except:
    dimmovie_df.write.format("delta").save("s3a://lakehouse/gold/dim_movie")


# --------------------------------------------------
# DIMENSION TABLE: dim_genre
# --------------------------------------------------
dim_genre_df = df_Movies.select(
    explode(col("genres")).alias("genre")
).select(
    col("genre.id").cast("integer").alias("id"),
    col("genre.name").alias("name")
).dropDuplicates(["id"])

try:
    dim_genre = DeltaTable.forPath(spark, "s3a://lakehouse/gold/dim_genre")
    dim_genre.alias("target").merge(
        dim_genre_df.alias("source"),
        "target.id = source.id"
    ).whenNotMatchedInsertAll().execute()
except:
    dim_genre_df.write.format("delta").save("s3a://lakehouse/gold/dim_genre")


# --------------------------------------------------
# BRIDGE TABLE: movie_genres
# --------------------------------------------------
movie_genre_df = df_Movies.select(
    col("id").alias("movie_id"),
    explode(col("genres")).alias("genre")
).select(
    col("genre.id").cast("integer").alias("genres_id"),
    col("movie_id").alias("id")
).dropDuplicates(["id","genres_id"])
try:
    movie_genre = DeltaTable.forPath(spark, "s3a://lakehouse/gold/movie_genre")
    movie_genre.alias("target").merge(
        movie_genre_df.alias("source"),
        "target.id = source.id AND target.genres_id = source.genres_id"
    ).whenNotMatchedInsertAll().execute()
except:
    movie_genre_df.write.format("delta").save("s3a://lakehouse/gold/movie_genre")
    

# --------------------------------------------------
# DIMENSION TABLE: dim_keyword
# --------------------------------------------------


dim_keyword_df = df_Keywords.select(
    explode(col("keywords")).alias("keyword")
).select(
    col("keyword.id").alias("id"),
    col("keyword.name").alias("name")
).dropDuplicates(["id"])

try:
    dim_keyword = DeltaTable.forPath(spark, "s3a://lakehouse/gold/dim_keyword")
    dim_keyword.alias("target").merge(
        dim_keyword_df.alias("source"),
        "target.id = source.id"
    ).whenNotMatchedInsertAll().execute()
except:
    dim_keyword_df.write.format("delta").save("s3a://lakehouse/gold/dim_keyword")

# --------------------------------------------------
# BRIDGE TABLE: movie_keyword
# --------------------------------------------------
movie_keyword_df = df_Keywords.select(
    col("id").alias("movie_id"),
    explode(col("keywords")).alias("keyword")
).select(
    col("keyword.id").alias("keyword_id"),
    col("movie_id").alias("id"),
    col("keyword.name").alias("name")
).dropDuplicates(["id","keyword_id"])
try:
    movie_keyword = DeltaTable.forPath(spark, "s3a://lakehouse/gold/movie_keyword")
    movie_keyword.alias("target").merge(
        movie_keyword_df.alias("source"),
        "target.id = source.id AND target.keyword_id = source.keyword_id"
    ).whenNotMatchedInsertAll().execute()
except:
    movie_keyword_df.write.format("delta").save("s3a://lakehouse/gold/movie_keyword")


# --------------------------------------------------
# DIMENSION TABLE: dim_cast
# --------------------------------------------------
dim_cast_df = df_Crews.select(
    explode(col("cast")).alias("cast")
    ).select(
        col("cast.id").alias("id"),
        col("cast.name").alias("name"),
        col("cast.profile_path").alias("profile_path"),
        col("cast.gender").alias("gender")
).dropDuplicates(["id"])

try:
    dim_cast = DeltaTable.forPath(spark, "s3a://lakehouse/gold/dim_cast")
    dim_cast.alias("target").merge(
        dim_cast_df.alias("source"),
        "target.id = source.id"
    ).whenNotMatchedInsertAll().execute()
except:
    dim_cast_df.write.format("delta").save("s3a://lakehouse/gold/dim_cast")
    
# --------------------------------------------------
# BRIDGE TABLE: movie_cast
# --------------------------------------------------
movie_cast_df = df_Crews.select(
    col("id").alias("movie_id"),
    explode(col("cast")).alias("cast")
).select(
    col("cast.id").alias("cast_id"),
    col("cast.character").alias("character"),
    col("movie_id"),
    col("cast.order")
).dropDuplicates(["movie_id", "cast_id","character"])
try:
    movie_cast = DeltaTable.forPath(spark, "s3a://lakehouse/gold/movie_cast")
    movie_cast.alias("target").merge(
        movie_cast_df.alias("source"),
        "target.movie_id = source.movie_id AND target.cast_id = source.cast_id AND target.character = source.character"
    ).whenNotMatchedInsertAll().execute()
except:
    movie_cast_df.write.format("delta").save("s3a://lakehouse/gold/movie_cast")



# --------------------------------------------------
# DIMENSION TABLE: dim_crew
# --------------------------------------------------
dim_crew_df = df_Crews.select(
    explode(col("crew")).alias("crew")
    ).select(
        col("crew.id"),
        col("crew.name"),
        col("crew.profile_path"),
        col("crew.gender")

).dropDuplicates(["id"])
try:
    dim_crew = DeltaTable.forPath(spark, "s3a://lakehouse/gold/dim_crew")
    dim_crew.alias("target").merge(
        dim_crew_df.alias("source"),
        "target.id = source.id"
    ).whenNotMatchedInsertAll().execute()
except:
    dim_crew_df.write.format("delta").save("s3a://lakehouse/gold/dim_crew")
    
# --------------------------------------------------
# BRIDGE TABLE: movie_crew
# --------------------------------------------------
movie_crew_df = df_Crews.select(
    col("id").alias("movie_id"),
    explode(col("crew")).alias("crew")
).select(
    col("crew.id").alias("crew_id"),
    col("crew.job").alias("job"),
    col("movie_id"),
    col("crew.department").alias("department")
).dropDuplicates(["movie_id","crew_id","job","department"])
try:
    movie_crew = DeltaTable.forPath(spark, "s3a://lakehouse/gold/movie_crew")
    movie_crew.alias("target").merge(
        movie_crew_df.alias("source"),
        "target.movie_id = source.movie_id AND target.crew_id = source.crew_id AND target.job = source.job AND target.department = source.department"
    ).whenNotMatchedInsertAll().execute()
except:
    movie_crew_df.write.format("delta").save("s3a://lakehouse/gold/movie_crew")








max_read_time_row_movies = df_Movies.agg(max("read_time")).collect()[0][0]
max_read_time_row_crews = df_Crews.agg(max("read_time")).collect()[0][0]
max_read_time_row_keywords = df_Keywords.agg(max("read_time")).collect()[0][0]
try:
    new_rows = [
        ("BatchApi_Process_Movies",   max_read_time_row_movies),
        ("BatchApi_Process_Crews",    max_read_time_row_crews),
        ("BatchApi_Process_Keywords", max_read_time_row_keywords)
    ]
    new_df = spark.createDataFrame(new_rows, ["task_id", "last_read_time"])
    readTime = spark.read.format("delta").load("s3a://lakehouse/ReadTime")
    filtered_readTime = readTime.filter(
        "task_id NOT IN ('BatchApi_Process_Movies', 'BatchApi_Process_Crews', 'BatchApi_Process_Keywords')"
    )
    updated_df = filtered_readTime.union(new_df)
    updated_df.write.format("delta").mode("overwrite").save("s3a://lakehouse/ReadTime")
except Exception as e:
    print("Đã xảy ra lỗi khi cập nhật ReadTime:", str(e))