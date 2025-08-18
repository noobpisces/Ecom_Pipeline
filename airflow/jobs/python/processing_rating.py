import os
import sys
import traceback
import logging
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

def init_spark_session():
    try:
        logging.info("Initializing Spark session...")
        spark = SparkSession.builder \
            .appName('CleanRatings') \
            .master('spark://spark-master:7077') \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "conbo123") \
            .config("spark.hadoop.fs.s3a.secret.key", "123conbo") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
            .config('spark.sql.warehouse.dir', "s3a://lakehouse/") \
            .getOrCreate()

        logging.info("Spark session initialized successfully.")
        return spark
    except Exception as e:
        logging.error(f"Error initializing Spark session: {str(e)}")
        logging.error(traceback.format_exc())  # Log the full error details
        sys.exit(1)

def silver_cleaned_ratings(input_path: str, output_path: str):
    """
    Load ratings table from the bronze layer in MinIO, clean the data with custom transformations,
    and save to the Silver layer in Delta format.
    """
    try:
        # Initialize Spark session
        spark = init_spark_session()
        logging.info("Start cleaning ratings table")

        # Define the schema for the ratings data
        ratings_schema = StructType([
            StructField("userId", IntegerType(), nullable=True),
            StructField("movieId", IntegerType(), nullable=True),
            StructField("rating", FloatType(), nullable=True),
            StructField("timestamp", IntegerType(), nullable=True)
        ])

        # Read data from Parquet without initially applying the schema
        logging.info(f"Reading data from {input_path}")
        df = spark.read.format("parquet").load(input_path)

        # Convert to DataFrame with schema explicitly applied
        df = spark.createDataFrame(df.rdd, schema=ratings_schema)

        # Drop rows with any null values in key columns
        logging.info("Dropping rows with null values")
        df_cleaned = df.na.drop()

        # Count rows after cleaning
        logging.info(f"Row count after cleaning: {df_cleaned.count()}")

        # Save cleaned data to Delta Lake on MinIO
        logging.info(f"Saving cleaned data to {output_path}")
        df_cleaned.write.format("delta").mode("overwrite").option('overwriteSchema', 'true').save(output_path)

        logging.info("Data cleaning and saving process completed successfully.")

    except Exception as e:
        logging.error(f"Error during cleaning process: {str(e)}")
        logging.error(traceback.format_exc())  # Log the full error details
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == '__main__':
    try:
        # Lấy đường dẫn đầu vào và đầu ra từ command line arguments
        if len(sys.argv) != 3:
            logging.error("Usage: spark-submit clean_ratings.py <input_path> <output_path>")
            sys.exit(1)

        input_path = sys.argv[1]  # Nhận đường dẫn đầu vào từ Airflow
        output_path = sys.argv[2]  # Nhận đường dẫn đầu ra từ Airflow

        silver_cleaned_ratings(input_path, output_path)
    
    except Exception as e:
        logging.error(f"Error in main execution: {str(e)}")
        logging.error(traceback.format_exc())  # Log the full error details
        sys.exit(1)
