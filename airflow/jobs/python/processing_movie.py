


import os
import sys
import traceback
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr,when,to_date
from pyspark.sql.types import StructType, StructField, BooleanType, StringType, IntegerType, DateType, FloatType,DoubleType
from pyspark.sql import functions as F
# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

def init_spark_session():
    try:
        logging.info("Initializing Spark session...")
        spark = SparkSession.builder \
            .appName('CleanMovies') \
            .master('spark://spark-master:7077') \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "conbo123") \
            .config("spark.hadoop.fs.s3a.secret.key", "123conbo") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
            .config("delta.enable-non-concurrent-writes", "true") \
            .config('spark.sql.warehouse.dir', "s3a://lakehouse/") \
            .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4') \
            .getOrCreate()

        logging.info("Spark session initialized successfully.")
        return spark
    except Exception as e:
        logging.error(f"Error initializing Spark session: {str(e)}")
        logging.error(traceback.format_exc())
        sys.exit(1)

def clean_movies(input_path, output_path):
    try:
        # Initialize SparkSession
        spark = init_spark_session()
        # Read data from Parquet
        logging.info(f"Reading data from {input_path}")
        df = spark.read.format("parquet").load(input_path)
        
        # df = df.filter(
        #                         # (col("budget") != "0") &            # loại bỏ dòng có budget là "0" (kiểu string)
        #                         (col("id").isNotNull()) &           # loại bỏ dòng có id là null
        #                         (col("revenue") != 0) &             # loại bỏ dòng có revenue bằng 0
        #                         (col("vote_average") != 0) &        # loại bỏ dòng có vote_average bằng 0
        #                         (col("vote_count") != 0) &          # loại bỏ dòng có vote_count bằng 0
        #                         (col("popularity") != "0") &        # loại bỏ dòng có popularity là "0" (kiểu string)
        #                         (col("release_date").isNotNull()) & # loại bỏ dòng có release_date là null
        #                         (col("runtime") != 0)               # loại bỏ dòng có runtime bằng 0
        #                     )
        df = df.filter(
                        (col("id").isNotNull()) &          
                        (col("release_date").isNotNull())
                    )
        df = df.dropDuplicates(["id"])

        logging.info(f"Saving cleaned data to {output_path}")
        df.write.format("delta").mode("overwrite").save(output_path)
        return df

    except Exception as e:
        logging.error(f"Error during cleaning process: {str(e)}")
        logging.error(traceback.format_exc())
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == '__main__':
    try:
        # Lấy đường dẫn đầu vào và đầu ra từ command line arguments
        if len(sys.argv) != 3:
            logging.error("Usage: spark-submit clean_movies.py <input_path> <output_path>")
            sys.exit(1)

        input_path = sys.argv[1]  # Nhận đường dẫn đầu vào từ Airflow
        output_path = sys.argv[2]  # Nhận đường dẫn đầu ra từ Airflow

        clean_movies(input_path, output_path)
    
    except Exception as e:
        logging.error(f"Error in main execution: {str(e)}")
        logging.error(traceback.format_exc())
        sys.exit(1)
