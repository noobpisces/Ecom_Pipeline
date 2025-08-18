from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import logging
from datetime import datetime
from airflow import DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 28)
}
with DAG(
    dag_id='SelectDataCB',
    default_args=default_args,
    schedule_interval='0 6 * * *',
    catchup=False,
) as dag:
    DataCB = SparkSubmitOperator(
task_id='SelectDataCB',
application='/opt/airflow/jobs/python/selectData.py',
jars="/opt/airflow/jars/hadoop-aws-3.3.4.jar,"
     "/opt/airflow/jars/s3-2.18.41.jar,"
     "/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,"
     "/opt/airflow/jars/aws-java-sdk-1.12.367.jar,"
     "/opt/airflow/jars/delta-core_2.12-2.2.0.jar,"
     "/opt/airflow/jars/delta-storage-2.2.0.jar",
conn_id="spark-conn",
    conf={
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.executor.memory": "4g",  # Tăng RAM cho mỗi executor
        "spark.driver.memory": "4g",  # Tăng RAM cho driver (nếu cần)
        "spark.executor.cores": "4",  # Sử dụng 2 core/executor
        "spark.driver.cores": "4",  # Sử dụng 2 core cho driver
        "spark.dynamicAllocation.enabled": "false",  # Tắt tự động cấp phát (manual tuning)
        "spark.sql.shuffle.partitions": "8",  # Tăng số partitions khi shuffle
    }
)

    DataCB