import os
import sys
import traceback
import logging
import pandas as pd
import boto3
from io import BytesIO
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import logging
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# Hàm kết nối MinIO
def connect_minio():
    try:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id="conbo123",
            aws_secret_access_key="123conbo",
            endpoint_url='http://minio:9000' 
        )
        return s3_client
    except Exception as e:
        logging.error(f"Error connecting to MinIO: {str(e)}")
        raise e

# Hàm lấy dữ liệu từ MinIO
def get_data_from_raw(name):
    try:
        client = connect_minio()
        response = client.get_object(Bucket="lakehouse", Key=f'raw/{name}.csv')
        df = pd.read_csv(response.get("Body"), low_memory=False)
        return df
    except Exception as e:
        logging.error(f"Error getting data from MinIO: {str(e)}")
        raise e

# Hàm ghi dữ liệu vào MinIO
def save_data_to_bronze(df, name):
    try:
        client = connect_minio()
        # Sử dụng BytesIO để lưu trữ dữ liệu dưới dạng Parquet
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)  # Reset buffer position
        client.put_object(Bucket="lakehouse", Key=f'bronze/{name}.parquet', Body=parquet_buffer.getvalue())
        logging.info(f"Data saved to bronze/{name}.parquet successfully.")
    except Exception as e:
        logging.error(f"Error saving data to MinIO: {str(e)}")
        raise e

# Các task sẽ chạy dưới dạng Python Operator
def bronze_keywords(**kwargs):
    try:
        df = get_data_from_raw('keywords')
        logging.info(f"Table extracted with shape: {df.shape}")
        save_data_to_bronze(df, 'keywords')
    except Exception as e:
        logging.error(f"Error in bronze_keywords task: {str(e)}")
        raise e

def bronze_movies(**kwargs):
    try:
        df = get_data_from_raw('movies_metadata')
        logging.info(f"Table extracted with shape: {df.shape}")
        save_data_to_bronze(df, 'movies')
    except Exception as e:
        logging.error(f"Error in bronze_movies task: {str(e)}")
        raise e

def bronze_credits(**kwargs):
    try:
        df = get_data_from_raw('credits')
        logging.info(f"Table extracted with shape: {df.shape}")
        save_data_to_bronze(df, 'credits')
    except Exception as e:
        logging.error(f"Error in bronze_credits task: {str(e)}")
        raise e

def bronze_ratings(**kwargs):
    try:
        df = get_data_from_raw('ratings')
        logging.info(f"Table extracted with shape: {df.shape}")
        save_data_to_bronze(df, 'ratings')
    except Exception as e:
        logging.error(f"Error in bronze_ratings task: {str(e)}")
        raise e

def bronze_links(**kwargs):
    try:
        df = get_data_from_raw('links')
        logging.info(f"Table extracted with shape: {df.shape}")
        save_data_to_bronze(df, 'links')
    except Exception as e:
        logging.error(f"Error in bronze_links task: {str(e)}")
        raise e

def bronze_review(**kwargs):
    try:
        df = get_data_from_raw("IMDB_Dataset")
        logging.info(f"Table extracted with shape: {df.shape}")
        save_data_to_bronze(df, 'review')
    except Exception as e:
        logging.error(f"Error in bronze_review task: {str(e)}")
        raise e

# Định nghĩa DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'All_Layer',
    default_args=default_args,
    description='A simple DAG to process raw data to bronze layer',
    schedule_interval=None,  
    catchup=False
) as dag:


    
    # Định nghĩa các task
    task_bronze_keywords = PythonOperator(
        task_id='bronze_keywords',
        python_callable=bronze_keywords,
        provide_context=True
    )

    task_bronze_movies = PythonOperator(
        task_id='bronze_movies',
        python_callable=bronze_movies,
        provide_context=True
    )

    task_bronze_credits = PythonOperator(
        task_id='bronze_credits',
        python_callable=bronze_credits,
        provide_context=True
    )

    # task_bronze_ratings = PythonOperator(
    #     task_id='bronze_ratings',
    #     python_callable=bronze_ratings,
    #     provide_context=True
    # )

    # task_bronze_links = PythonOperator(
    #     task_id='bronze_links',
    #     python_callable=bronze_links,
    #     provide_context=True
    # )

    # task_bronze_review = PythonOperator(
    #     task_id='bronze_review',
    #     python_callable=bronze_review,
    #     provide_context=True
    # )
    clean_keywords = SparkSubmitOperator(
        task_id='clean_keyword',
        application="/opt/airflow/jobs/python/processing_keyword.py",  # Đường dẫn đến file Python Spark
        application_args=["s3a://lakehouse/bronze/keywords.parquet", "s3a://lakehouse/silver/keywords"],  # Truyền input và output từ Airflow
        jars="/opt/airflow/jars/hadoop-aws-3.3.4.jar,"
             "/opt/airflow/jars/s3-2.18.41.jar,"
             "/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,"
             "/opt/airflow/jars/aws-java-sdk-1.12.367.jar,"
             "/opt/airflow/jars/delta-core_2.12-2.2.0.jar,"
             "/opt/airflow/jars/delta-storage-2.2.0.jar,",  # Đường dẫn JARs cần thiết
        conn_id="spark-conn",  # Kết nối Spark
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.executor.memory": "5g",  # Tăng RAM cho mỗi executor
            "spark.driver.memory": "2g",  # Tăng RAM cho driver (nếu cần)
            "spark.executor.cores": "2",  # Sử dụng 2 core/executor
            "spark.driver.cores": "2",  # Sử dụng 2 core cho driver
            "spark.dynamicAllocation.enabled": "false",  # Tắt tự động cấp phát (manual tuning)
            "spark.sql.shuffle.partitions": "8",  # Tăng số partitions khi shuffle
        }
    )


    # clean_ratings = SparkSubmitOperator(
    #     task_id='clean_rating',
    #     application="/opt/airflow/jobs/python/processing_rating.py",  # Đường dẫn đến file Python Spark
    #     application_args=["s3a://lakehouse/bronze/ratings.parquet", "s3a://lakehouse/silver/ratings"],  # Truyền input và output từ Airflow
    #     jars="/opt/airflow/jars/hadoop-aws-3.3.4.jar,"
    #          "/opt/airflow/jars/s3-2.18.41.jar,"
    #          "/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,"
    #          "/opt/airflow/jars/aws-java-sdk-1.12.367.jar,"
    #          "/opt/airflow/jars/delta-core_2.12-2.2.0.jar,"
    #          "/opt/airflow/jars/delta-storage-2.2.0.jar,",  # Đường dẫn JARs cần thiết
    #     conn_id="spark-conn",  # Kết nối Spark
    #     conf={
    #         "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    #         "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    #         "spark.executor.memory": "5g",  # Tăng RAM cho mỗi executor
    #         "spark.driver.memory": "2g",  # Tăng RAM cho driver (nếu cần)
    #         "spark.executor.cores": "2",  # Sử dụng 2 core/executor
    #         "spark.driver.cores": "2",  # Sử dụng 2 core cho driver
    #         "spark.dynamicAllocation.enabled": "false",  # Tắt tự động cấp phát (manual tuning)
    #         "spark.sql.shuffle.partitions": "8",  # Tăng số partitions khi shuffle
    #     }
    # )

    # clean_review = SparkSubmitOperator(
    #     task_id='clean_review',
    #     application="/opt/airflow/jobs/python/processing_review.py",  # Đường dẫn đến file Python Spark
    #     application_args=["s3a://lakehouse/bronze/review.parquet", "s3a://lakehouse/silver/review"],  # Truyền input và output từ Airflow
    #     jars="/opt/airflow/jars/hadoop-aws-3.3.4.jar,"
    #          "/opt/airflow/jars/s3-2.18.41.jar,"
    #          "/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,"
    #          "/opt/airflow/jars/aws-java-sdk-1.12.367.jar,"
    #          "/opt/airflow/jars/delta-core_2.12-2.2.0.jar,"
    #          "/opt/airflow/jars/delta-storage-2.2.0.jar,",  # Đường dẫn JARs cần thiết
    #     conn_id="spark-conn",  # Kết nối Spark
    #     conf={
    #         "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    #         "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    #         "spark.executor.memory": "5g",  # Tăng RAM cho mỗi executor
    #         "spark.driver.memory": "2g",  # Tăng RAM cho driver (nếu cần)
    #         "spark.executor.cores": "2",  # Sử dụng 2 core/executor
    #         "spark.driver.cores": "2",  # Sử dụng 2 core cho driver
    #         "spark.dynamicAllocation.enabled": "false",  # Tắt tự động cấp phát (manual tuning)
    #         "spark.sql.shuffle.partitions": "8",  # Tăng số partitions khi shuffle
    #     }
    # )

    clean_credit = SparkSubmitOperator(
        task_id='clean_credit',
        application="/opt/airflow/jobs/python/processing_credit.py",  # Đường dẫn đến file Python Spark
        application_args=["s3a://lakehouse/bronze/credits.parquet", "s3a://lakehouse/silver/credit"],  # Truyền input và output từ Airflow
        jars="/opt/airflow/jars/hadoop-aws-3.3.4.jar,"
             "/opt/airflow/jars/s3-2.18.41.jar,"
             "/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,"
             "/opt/airflow/jars/aws-java-sdk-1.12.367.jar,"
             "/opt/airflow/jars/delta-core_2.12-2.2.0.jar,"
             "/opt/airflow/jars/delta-storage-2.2.0.jar,",  # Đường dẫn JARs cần thiết
        conn_id="spark-conn",  # Kết nối Spark
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.executor.memory": "5g",  # Tăng RAM cho mỗi executor
            "spark.driver.memory": "2g",  # Tăng RAM cho driver (nếu cần)
            "spark.executor.cores": "2",  # Sử dụng 2 core/executor
            "spark.driver.cores": "2",  # Sử dụng 2 core cho driver
            "spark.dynamicAllocation.enabled": "false",  # Tắt tự động cấp phát (manual tuning)
            "spark.sql.shuffle.partitions": "8",  # Tăng số partitions khi shuffle
        }
    )


    cleaned_movies = SparkSubmitOperator(
        task_id='clean_movies',
        application="/opt/airflow/jobs/python/processing_movie.py",  # Đường dẫn đến file Python Spark
        application_args=["s3a://lakehouse/bronze/movies.parquet", "s3a://lakehouse/silver/movies"],  # Truyền input và output từ Airflow
        jars="/opt/airflow/jars/hadoop-aws-3.3.4.jar,"
             "/opt/airflow/jars/s3-2.18.41.jar,"
             "/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,"
             "/opt/airflow/jars/aws-java-sdk-1.12.367.jar,"
             "/opt/airflow/jars/delta-core_2.12-2.2.0.jar,"
             "/opt/airflow/jars/delta-storage-2.2.0.jar,",  # Đường dẫn JARs cần thiết
        conn_id="spark-conn",  # Kết nối Spark
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "delta.enable-non-concurrent-writes": "true" ,
            "spark.executor.memory": "5g",  # Tăng RAM cho mỗi executor
            "spark.driver.memory": "2g",  # Tăng RAM cho driver (nếu cần)
            "spark.executor.cores": "2",  # Sử dụng 2 core/executor
            "spark.driver.cores": "2",  # Sử dụng 2 core cho driver
            "spark.dynamicAllocation.enabled": "false",  # Tắt tự động cấp phát (manual tuning)
            "spark.sql.shuffle.partitions": "8",  # Tăng số partitions khi shuffle
        })
    
#------------------------------------------------------------------------------------------------------------------------------------
###
######
######################################################################################################################################################
###############################################################################################################################################################

    dimmovie = SparkSubmitOperator(
        task_id='dimmovie',
        application="/opt/airflow/jobs/python/dimmovie.py",  # Đường dẫn đến file Python Spark
        jars="/opt/airflow/jars/hadoop-aws-3.3.4.jar,"
             "/opt/airflow/jars/s3-2.18.41.jar,"
             "/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,"
             "/opt/airflow/jars/aws-java-sdk-1.12.367.jar,"
             "/opt/airflow/jars/delta-core_2.12-2.2.0.jar,"
             "/opt/airflow/jars/delta-storage-2.2.0.jar,",  # Đường dẫn JARs cần thiết
        conn_id="spark-conn",
        conf={
        "spark.executor.memory": "5g",  # Tăng RAM cho mỗi executor
        "spark.driver.memory": "2g",  # Tăng RAM cho driver (nếu cần)
        "spark.executor.cores": "2",  # Sử dụng 2 core/executor
        "spark.driver.cores": "2",  # Sử dụng 2 core cho driver
        "spark.dynamicAllocation.enabled": "false",  # Tắt tự động cấp phát (manual tuning)
        "spark.sql.shuffle.partitions": "8",  # Tăng số partitions khi shuffle
    }
    )
    dimkeyword = SparkSubmitOperator(
        task_id='dimkeyword',
        application="/opt/airflow/jobs/python/dimkeyword.py",  # Đường dẫn đến file Python Spark
        jars="/opt/airflow/jars/hadoop-aws-3.3.4.jar,"
             "/opt/airflow/jars/s3-2.18.41.jar,"
             "/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,"
             "/opt/airflow/jars/aws-java-sdk-1.12.367.jar,"
             "/opt/airflow/jars/delta-core_2.12-2.2.0.jar,"
             "/opt/airflow/jars/delta-storage-2.2.0.jar,",  # Đường dẫn JARs cần thiết
        conn_id="spark-conn",
        conf={
        "spark.executor.memory": "5g",  # Tăng RAM cho mỗi executor
        "spark.driver.memory": "2g",  # Tăng RAM cho driver (nếu cần)
        "spark.executor.cores": "2",  # Sử dụng 2 core/executor
        "spark.driver.cores": "2",  # Sử dụng 2 core cho driver
        "spark.dynamicAllocation.enabled": "false",  # Tắt tự động cấp phát (manual tuning)
        "spark.sql.shuffle.partitions": "8",  # Tăng số partitions khi shuffle
    }
    )
    dimcrew = SparkSubmitOperator(
        task_id='dimcrew',
        application="/opt/airflow/jobs/python/dimcrew.py",  # Đường dẫn đến file Python Spark
        jars="/opt/airflow/jars/hadoop-aws-3.3.4.jar,"
             "/opt/airflow/jars/s3-2.18.41.jar,"
             "/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,"
             "/opt/airflow/jars/aws-java-sdk-1.12.367.jar,"
             "/opt/airflow/jars/delta-core_2.12-2.2.0.jar,"
             "/opt/airflow/jars/delta-storage-2.2.0.jar,",  # Đường dẫn JARs cần thiết
        conn_id="spark-conn",
        conf={
        "spark.executor.memory": "5g",  # Tăng RAM cho mỗi executor
        "spark.driver.memory": "2g",  # Tăng RAM cho driver (nếu cần)
        "spark.executor.cores": "2",  # Sử dụng 2 core/executor
        "spark.driver.cores": "2",  # Sử dụng 2 core cho driver
        "spark.dynamicAllocation.enabled": "false",  # Tắt tự động cấp phát (manual tuning)
        "spark.sql.shuffle.partitions": "8",  # Tăng số partitions khi shuffle
    }
    )
    dimcast = SparkSubmitOperator(
        task_id='dimcast',
        application="/opt/airflow/jobs/python/dimcast.py",  # Đường dẫn đến file Python Spark
        jars="/opt/airflow/jars/hadoop-aws-3.3.4.jar,"
             "/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,"
             "/opt/airflow/jars/delta-core_2.12-2.2.0.jar,"
             "/opt/airflow/jars/delta-storage-2.2.0.jar,",  # Đường dẫn JARs cần thiết
        conn_id="spark-conn"
    )
    dimdate = SparkSubmitOperator(
        task_id='dimdate',
        application="/opt/airflow/jobs/python/dimdate.py",  # Đường dẫn đến file Python Spark
        jars="/opt/airflow/jars/hadoop-aws-3.3.4.jar,"
             "/opt/airflow/jars/s3-2.18.41.jar,"
             "/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,"
             "/opt/airflow/jars/aws-java-sdk-1.12.367.jar,"
             "/opt/airflow/jars/delta-core_2.12-2.2.0.jar,"
             "/opt/airflow/jars/delta-storage-2.2.0.jar,",  # Đường dẫn JARs cần thiết
        conn_id="spark-conn",
        conf={
        "spark.executor.memory": "5g",  # Tăng RAM cho mỗi executor
        "spark.driver.memory": "2g",  # Tăng RAM cho driver (nếu cần)
        "spark.executor.cores": "2",  # Sử dụng 2 core/executor
        "spark.driver.cores": "2",  # Sử dụng 2 core cho driver
        "spark.dynamicAllocation.enabled": "false",  # Tắt tự động cấp phát (manual tuning)
        "spark.sql.shuffle.partitions": "8",  # Tăng số partitions khi shuffle
    }
    )
    dimgenre = SparkSubmitOperator(
        task_id='dimgenre',
        application="/opt/airflow/jobs/python/dimgenre.py",  # Đường dẫn đến file Python Spark
        jars="/opt/airflow/jars/hadoop-aws-3.3.4.jar,"
             "/opt/airflow/jars/s3-2.18.41.jar,"
             "/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,"
             "/opt/airflow/jars/aws-java-sdk-1.12.367.jar,"
             "/opt/airflow/jars/delta-core_2.12-2.2.0.jar,"
             "/opt/airflow/jars/delta-storage-2.2.0.jar,",  # Đường dẫn JARs cần thiết
        conn_id="spark-conn",
        conf={
        "spark.executor.memory": "5g",  # Tăng RAM cho mỗi executor
        "spark.driver.memory": "2g",  # Tăng RAM cho driver (nếu cần)
        "spark.executor.cores": "2",  # Sử dụng 2 core/executor
        "spark.driver.cores": "2",  # Sử dụng 2 core cho driver
        "spark.dynamicAllocation.enabled": "false",  # Tắt tự động cấp phát (manual tuning)
        "spark.sql.shuffle.partitions": "8",  # Tăng số partitions khi shuffle
    }
    )
    factmovie = SparkSubmitOperator(
        task_id='factmovie',
        application="/opt/airflow/jobs/python/factmovie.py",  # Đường dẫn đến file Python Spark
        jars="/opt/airflow/jars/hadoop-aws-3.3.4.jar,"
             "/opt/airflow/jars/s3-2.18.41.jar,"
             "/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,"
             "/opt/airflow/jars/aws-java-sdk-1.12.367.jar,"
             "/opt/airflow/jars/delta-core_2.12-2.2.0.jar,"
             "/opt/airflow/jars/delta-storage-2.2.0.jar,",  # Đường dẫn JARs cần thiết
        conn_id="spark-conn",
        conf={
        "spark.executor.memory": "5g",  # Tăng RAM cho mỗi executor
        "spark.driver.memory": "2g",  # Tăng RAM cho driver (nếu cần)
        "spark.executor.cores": "2",  # Sử dụng 2 core/executor
        "spark.driver.cores": "2",  # Sử dụng 2 core cho driver
        "spark.dynamicAllocation.enabled": "false",  # Tắt tự động cấp phát (manual tuning)
        "spark.sql.shuffle.partitions": "8",  # Tăng số partitions khi shuffle
    }
    )


    # start>> task_bronze_credits >> complete_bronze >> clean_credit >> complete_silver>> dimcast >> dimcrew >> complete_gold >> end
    # start>> task_bronze_keywords >> complete_bronze >> clean_keywords>> complete_silver >> dimkeyword>>complete_gold >> end
    # start>> task_bronze_movies >> complete_bronze>> cleaned_movies>> complete_silver >> merge_data >>complete_gold >> end
    # start>> task_bronze_review>> complete_bronze >> clean_review>> complete_silver >> merge_data >>complete_gold >> end
    # start>> task_bronze_links >> complete_bronze

    # Thiết lập thứ tự chạy các task

    task_bronze_keywords >> task_bronze_movies >> task_bronze_credits >> \
    clean_keywords >>  clean_credit >> cleaned_movies >> \
    dimkeyword >> dimcrew >> dimcast >> dimgenre >> dimdate >> dimmovie >> factmovie
