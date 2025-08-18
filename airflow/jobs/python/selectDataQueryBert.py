
import os
import sys
import traceback
from pyspark.sql.functions import col, size
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_replace, broadcast, col, collect_set, concat_ws,
    sort_array
)
from pyspark.sql.types import *
from delta.tables import DeltaTable
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import length
# ====== 1. SparkSession cấu hình để làm việc với MinIO + Delta ======
spark = SparkSession.builder \
    .appName("MinIO with Delta Lake") \
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

# ====== 2. Load các bảng từ tầng gold ======
fact_movies = spark.read.format("delta").load("s3a://lakehouse/gold/fact_movies")
dim_movie = spark.read.format("delta").load("s3a://lakehouse/gold/dim_movie")
dim_cast = broadcast(spark.read.format("delta").load("s3a://lakehouse/gold/dim_cast"))
movie_cast = spark.read.format("delta").load("s3a://lakehouse/gold/movie_cast")
movie_genre = spark.read.format("delta").load("s3a://lakehouse/gold/movie_genre")
dim_genre = broadcast(spark.read.format("delta").load("s3a://lakehouse/gold/dim_genre"))
movie_crew = spark.read.format("delta").load("s3a://lakehouse/gold/movie_crew")
dim_crew = broadcast(spark.read.format("delta").load("s3a://lakehouse/gold/dim_crew"))
dim_keyword = broadcast(spark.read.format("delta").load("s3a://lakehouse/gold/dim_keyword"))
movie_keyword = spark.read.format("delta").load("s3a://lakehouse/gold/movie_keyword")


from pyspark.sql import functions as F

# cast_agg: lấy top 2 cast, giữ nguyên tên
cast_agg = (
    movie_cast.alias("mca")
    .filter(F.col("mca.order") <= 3)
    .join(dim_cast.alias("dca"), F.col("mca.cast_id") == F.col("dca.id"), "inner")
    .groupBy(F.col("mca.movie_id"))
    .agg(F.sort_array(F.collect_set(F.col("dca.name"))).alias("cast_names"))
    .withColumnRenamed("movie_id", "id")
)

# keyword_agg: gom keyword, giữ nguyên tên
keyword_agg = (
    movie_keyword.alias("mk")
    .join(dim_keyword.alias("dk"), F.col("mk.keyword_id") == F.col("dk.id"), "inner")
    .groupBy(F.col("mk.id"))   # CHỈNH SỬA: group by movie_id, không phải id
    .agg(F.sort_array(F.collect_set(F.col("dk.name"))).alias("keyword_names"))
)

# crew_agg: lấy director, giữ nguyên tên
crew_agg = (
    movie_crew.alias("mcr")
    .filter(F.col("mcr.job") == "Director")
    .join(dim_crew.alias("dcr"), F.col("mcr.crew_id") == F.col("dcr.id"), "inner")
    .groupBy(F.col("mcr.movie_id"))
    .agg(F.sort_array(F.collect_set(F.col("dcr.name"))).alias("director_names"))
    .withColumnRenamed("movie_id", "id")
)

genre_agg = (
    movie_genre.alias("mg")
    .join(dim_genre.alias("dg"), col("mg.genres_id") == col("dg.id"), "inner")
    .groupBy(col("mg.id"))
    .agg(sort_array(collect_set(col("dg.name"))).alias("genres"))
)

# ====== 4. Join với fact_movies và xử lý dữ liệu tổng hợp ======
result_df = (
    fact_movies.alias("fm")
    .join(cast_agg.alias("ca"), col("fm.id") == col("ca.id"), "left")  
    .join(crew_agg.alias("cr"), col("fm.id") == col("cr.id"), "left")  
    .join(genre_agg.alias("ga"), col("fm.id") == col("ga.id"), "left")
    .join(keyword_agg.alias("ka"), col("fm.id") == col("ka.id"), "left")
    .join(dim_movie.alias("dm"), col("fm.id") == col("dm.id"),"left")
    .select(
        col("fm.id"),col("dm.title"),col("dm.tagline"), col("fm.budget"), col("fm.popularity"), col("fm.revenue"),
        col("fm.vote_average"), col("fm.vote_count"), col("fm.date_id"),
        col("ca.cast_names"), col("cr.director_names"),
        col("ga.genres"), col("ka.keyword_names"),col("dm.overview")
    )
)


# UDF tạo nội dung mô tả đầy đủ cho comb_bert
# def create_bert_input(keywords, cast_names, director_names, genres, overview, title, tagline):
#     def to_str(x): return ", ".join(x) if x else ""
    
#     title = title or ""
#     tagline = tagline or ""
#     genres_str = to_str(genres)
#     cast_str = to_str(cast_names)
#     director_str = to_str(director_names)
#     keyword_str = to_str(keywords)
#     overview = overview.strip() if overview else ""

#     return (
#         f"Title: {title}. "
#         f"This movie is titled '{title}'. "
#         f"Tagline: {tagline}. "
#         f"Tagline repeated: {tagline}. "
#         f"Genre: {genres_str}. "
#         f"Directed by: {director_str}. "
#         f"Starring: {cast_str}. "
#         f"Important keywords include: {keyword_str}. "
#         f"Also about: {keyword_str}. "
#         f"Plot summary: {overview}. "
#         f"This is a story about: {overview}. "
#     )

# from pyspark.sql.functions import udf
# from pyspark.sql.types import StringType

# bert_input_udf = udf(create_bert_input, StringType())
# filtered_df = result_df.filter(
#     (col("keyword_names").isNotNull()) & (size(col("keyword_names")) > 0) &
#     (col("cast_names").isNotNull()) & (size(col("cast_names")) > 0) &
#     (col("director_names").isNotNull()) & (size(col("director_names")) > 0) &
#     (col("genres").isNotNull()) & (size(col("genres")) > 0) &
#     (col("overview").isNotNull()) & (col("overview") != "") &
#     (col("title").isNotNull()) & (col("title") != "") &
#     (col("tagline").isNotNull()) & (col("tagline") != "")
# )

# # Sau đó mới apply UDF
# result_df = filtered_df.withColumn("comb_bert", bert_input_udf(
#     col("keyword_names"),
#     col("cast_names"),
#     col("director_names"),
#     col("genres"),
#     col("overview"),
#     col("title"),
#     col("tagline")
# ))
def create_bert_input(title, tagline, overview):
    title = title or ""
    tagline = tagline or ""
    overview = overview.strip() if overview else ""

    return (
        f"Title: {title}. "
        f"This movie is titled '{title}'. "
        f"Tagline: {tagline}. "
        f"Tagline repeated: {tagline}. "
        f"Plot summary: {overview}. "
        f"This is a story about: {overview}. "
    )
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

bert_input_udf = udf(create_bert_input, StringType())

from pyspark.sql.functions import col

filtered_df = result_df.filter(
    (col("overview").isNotNull()) & (col("overview") != "") &
    (col("title").isNotNull()) & (col("title") != "") &
    (col("tagline").isNotNull()) & (col("tagline") != "")
)
result_df = filtered_df.withColumn("comb_bert", bert_input_udf(
    col("title"),
    col("tagline"),
    col("overview")
))
# Ghi xuống Delta Lake
result_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("s3a://lakehouse/gold/machineDataBert")