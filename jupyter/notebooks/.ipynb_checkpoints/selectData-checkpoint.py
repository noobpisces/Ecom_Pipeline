# import os
# import sys
# import traceback
# import logging
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import  regexp_replace,udf,explode,col, expr,when,to_date, sum, from_json,size,length, collect_set, broadcast,concat_ws
# from pyspark.sql.types import  ArrayType,StructType, StructField, BooleanType, StringType, IntegerType, DateType, FloatType,DoubleType, LongType
# from delta.tables import DeltaTable
# from pyspark.sql import functions as F
# import json


# spark = SparkSession.builder \
#     .appName("MinIO with Delta Lake") \
#     .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
#     .config("spark.hadoop.fs.s3a.access.key", "conbo123") \
#     .config("spark.hadoop.fs.s3a.secret.key", "123conbo") \
#     .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
#     .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
#     .config("delta.enable-non-concurrent-writes", "true") \
#     .config('spark.sql.warehouse.dir', "s3a://lakehouse/") \
#     .getOrCreate()
    

# fact_movies = spark.read.format("delta").load("s3a://lakehouse/gold/fact_movies")
# # dim_movie = spark.read.format("delta").load("s3a://lakehouse/gold/dim_movie")
# dim_cast = spark.read.format("delta").load("s3a://lakehouse/gold/dim_cast")
# movie_cast = spark.read.format("delta").load("s3a://lakehouse/gold/movie_cast")
# movie_genre = spark.read.format("delta").load("s3a://lakehouse/gold/movie_genre")
# dim_genre = spark.read.format("delta").load("s3a://lakehouse/gold/dim_genre")
# movie_crew = spark.read.format("delta").load("s3a://lakehouse/gold/movie_crew")
# dim_crew = spark.read.format("delta").load("s3a://lakehouse/gold/dim_crew")
# dim_keyword = spark.read.format("delta").load("s3a://lakehouse/gold/dim_keyword")
# movie_keyword = spark.read.format("delta").load("s3a://lakehouse/gold/movie_keyword")




# # )
# # Broadcast các bảng Dimension nhỏ để tối ưu hiệu suất
# # dim_movie = broadcast(dim_movie)
# dim_cast = broadcast(dim_cast)
# dim_crew = broadcast(dim_crew)
# dim_genre = broadcast(dim_genre)
# dim_keyword = broadcast(dim_keyword)
# # Nhóm dữ liệu trước khi JOIN vào fact_movies

# cast_agg = (
#     movie_cast.alias("mca")
#     .filter((col("mca.order") < 3))
#     .join(dim_cast.alias("dca"), col("mca.cast_id") == col("dca.id"), "inner")
#     .groupBy(col("mca.movie_id"))
#     .agg(collect_set(regexp_replace(col("dca.name"), " ", "")).alias("cast_names"))
#     .withColumnRenamed("movie_id", "id")
# )
# keyword_agg = (
#     movie_keyword.alias("mk")
#     .join(dim_keyword.alias("dk"), col("mk.keyword_id") == col("dk.id"), "inner")
#     .groupBy(col("mk.id"))  # Đảm bảo sử dụng đúng cột movie_id từ movie_cast
#     .agg(collect_set(regexp_replace(col("dk.name"), " ", "")).alias("keyword_names"))
# )


# crew_agg = (
#     movie_crew.alias("mcr")
#     .filter(col("mcr.job") == "Director")
#     .join(dim_crew.alias("dcr"), col("mcr.crew_id") == col("dcr.id"), "inner")
#     .groupBy(col("mcr.movie_id"))  # Đảm bảo đúng cột movie_id từ movie_crew
#     .agg(collect_set(regexp_replace(col("dcr.name"), " ", "")).alias("director_names"))
#     .withColumnRenamed("movie_id", "id")  # Đổi tên để trùng với fact_movies
# )


# genre_agg = (
#     movie_genre.alias("mg")
#     .join(dim_genre.alias("dg"), col("mg.genres_id") == col("dg.id"), "inner")
#     .groupBy(col("mg.id"))  # Sử dụng cột id từ movie_genre
#     .agg(collect_set(col("dg.name")).alias("genres"))
# )


# # Thực hiện JOIN với fact_movies
# result_df = (
#     fact_movies.alias("fm")
#     .join(cast_agg.alias("ca"), col("fm.id") == col("ca.id"), "left")  
#     .join(crew_agg.alias("cr"), col("fm.id") == col("cr.id"), "left")  
#     .join(genre_agg.alias("ga"), col("fm.id") == col("ga.id"), "left")
#     .join(keyword_agg.alias("ka"), col("fm.id") == col("ka.id"), "left")
#     .select(
#         col("fm.id"), col("fm.budget"), col("fm.popularity"), col("fm.revenue"),
#         col("fm.vote_average"), col("fm.vote_count"), col("fm.date_id"),
#         col("ca.cast_names"), col("cr.director_names"), col("ga.genres"),col("ka.keyword_names")
#     )
# )
# result_df = result_df.dropDuplicates(['id'])
# result_df = result_df.withColumn('comb', concat_ws(" ", col('keyword_names'), col('cast_names'), col('director_names'), col('genres')))
# temp_result = result_df
# result_df = result_df.select(['id','comb'])


# try:
#     machineData = DeltaTable.forPath(spark, "s3a://lakehouse/gold/machineData")
#     machineData.alias("target").merge(
#         result_df.alias("source"),
#         "target.id = source.id"
#     ).whenNotMatchedInsertAll().execute()
# except:
#     result_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("s3a://lakehouse/gold/machineData")



# try:
#     MergeData = DeltaTable.forPath(spark, "s3a://lakehouse/gold/MergeData")
#     MergeData.alias("target").merge(
#         temp_result.alias("source"),
#         "target.id = source.id"
#     ).whenNotMatchedInsertAll().execute()
# except:
#     temp_result.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("s3a://lakehouse/gold/MergeData")



import os
import sys
import traceback
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_replace, broadcast, col, collect_set, concat_ws,
    sort_array
)
from pyspark.sql.types import *
from delta.tables import DeltaTable

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

# ====== 3. Tạo các bảng dimension dạng aggregate và ổn định thứ tự ======
# cast_agg = (
#     movie_cast.alias("mca")
#     .filter(col("mca.order") < 3)
#     .join(dim_cast.alias("dca"), col("mca.cast_id") == col("dca.id"), "inner")
#     .groupBy(col("mca.movie_id"))
#     .agg(sort_array(collect_set(regexp_replace(col("dca.name"), " ", ""))).alias("cast_names"))
#     .withColumnRenamed("movie_id", "id")
# )

# keyword_agg = (
#     movie_keyword.alias("mk")
#     .join(dim_keyword.alias("dk"), col("mk.keyword_id") == col("dk.id"), "inner")
#     .groupBy(col("mk.id"))
#     .agg(sort_array(collect_set(regexp_replace(col("dk.name"), " ", ""))).alias("keyword_names"))
# )

# crew_agg = (
#     movie_crew.alias("mcr")
#     .filter(col("mcr.job") == "Director")
#     .join(dim_crew.alias("dcr"), col("mcr.crew_id") == col("dcr.id"), "inner")
#     .groupBy(col("mcr.movie_id"))
#     .agg(sort_array(collect_set(regexp_replace(col("dcr.name"), " ", ""))).alias("director_names"))
#     .withColumnRenamed("movie_id", "id")
# )
from pyspark.sql import functions as F

# cast_agg: lấy top 2 cast, giữ nguyên tên
cast_agg = (
    movie_cast.alias("mca")
    .filter(F.col("mca.order") < 3)
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
        col("fm.id"),col("dm.title"), col("fm.budget"), col("fm.popularity"), col("fm.revenue"),
        col("fm.vote_average"), col("fm.vote_count"), col("fm.date_id"),
        col("ca.cast_names"), col("cr.director_names"),
        col("ga.genres"), col("ka.keyword_names"),col("dm.overview")
    )
)

# ====== 5. Loại bỏ bản ghi trùng id (sau khi các mảng đã ổn định với sort_array) ======
result_df = result_df.dropDuplicates(['id'])
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# def create_bert_input_udf(keywords, cast_names, director_names, genres, overview):
#     genres = ", ".join(genres) if genres else ""
#     directors = ", ".join(director_names) if director_names else ""
#     cast = ", ".join(cast_names) if cast_names else ""
#     keys = ", ".join(keywords) if keywords else ""
#     overview = overview.strip() if overview else ""

#     return f"This is a {genres} movie directed by {directors}. " \
#            f"The main cast includes {cast}. " \
#            f"It features themes like {keys}. {overview}"

# bert_input_udf = udf(create_bert_input_udf, StringType())

# result_df = result_df.withColumn(
#     "comb_bert",
#     bert_input_udf(
#         col("keyword_names"),
#         col("cast_names"),
#         col("director_names"),
#         col("genres"),
#         col("overview")
#     )
# )
def create_bert_input_udf(keywords, cast_names, director_names, genres, overview):
    # Xử lý các trường có thể None
    genres = ", ".join(genres) if genres else "unknown genres"
    directors = ", ".join(director_names) if director_names else "unknown director"
    cast = ", ".join(cast_names) if cast_names else "unknown cast"
    keys = ", ".join(keywords) if keywords else "unknown keywords"
    overview = overview.strip() if overview else "no overview available"
    
    # Tạo chuỗi với [SEP] phân cách các thành phần
    return (
        f"Overview: {overview} [SEP] "
        f"Genres: {genres} [SEP] "
        f"Keywords: {keys} [SEP] "
        f"Directors: {directors} [SEP] "
        f"Cast: {cast}"
    )

bert_input_udf = udf(create_bert_input_udf, StringType())

result_df = result_df.withColumn(
    "comb_bert",
    bert_input_udf(
        col("keyword_names"),
        col("cast_names"),
        col("director_names"),
        col("genres"), 
        col("overview")
    )
)

# ====== 6. UDF chuẩn hóa cho Word2Vec ======
def create_w2v_input(keywords, cast, directors, genres, overview):
    def fix(lst):
        return [x.replace(" ", "_").lower() for x in lst] if lst else []
    kw = fix(keywords)
    ca = fix(cast)
    dr = fix(directors)
    gn = fix(genres)
    ov = overview.strip().lower() if overview else ""
    return " ".join(kw + ca + dr + gn) + " " + ov

w2v_input_udf = udf(create_w2v_input, StringType())

result_df = result_df.withColumn(
    "comb",
    w2v_input_udf(
        col("keyword_names"),
        col("cast_names"),
        col("director_names"),
        col("genres"),
        col("overview")
    )
)



# ====== 6. Tạo cột 'comb' dùng để vector hóa text cho model machine learning ======
# result_df = result_df.withColumn('comb', concat_ws(" ", col('keyword_names'), col('cast_names'), col('director_names'), col('genres'),col('overview')))

# ====== 7. Ghi ra machineData: chỉ chứa id + comb ======
try:
    machineData = DeltaTable.forPath(spark, "s3a://lakehouse/gold/mldata")
    machineData.alias("target").merge(
        result_df.select("id", "comb","comb_bert").alias("source"),
        "target.id = source.id"
    ).whenNotMatchedInsertAll().execute()
except:
    result_df.select("id", "comb","comb_bert").write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("s3a://lakehouse/gold/mldata")

# # ====== 8. Ghi đầy đủ metadata cho các mục đích khác vào MergeData ======
# try:
#     MergeData = DeltaTable.forPath(spark, "s3a://lakehouse/gold/MergeData")
#     MergeData.alias("target").merge(
#         result_df.alias("source"),
#         "target.id = source.id"
#     ).whenNotMatchedInsertAll().execute()
# except:
#     result_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("s3a://lakehouse/gold/MergeData")
