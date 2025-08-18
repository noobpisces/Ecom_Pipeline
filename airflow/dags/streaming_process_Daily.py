from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from airflow.operators.bash import BashOperator
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 28),
    'depends_on_past': False
}

with DAG(
    dag_id='tmdb_crawler_streaming_process_Daily',
    default_args=default_args,
    schedule_interval='0 0 * * *',  
    catchup=False
) as dag:

    remove_existing_container = BashOperator(
            task_id='remove_existing_tmdb_crawler-Daily',
            bash_command='docker rm -f tmdb-crawler-Daily || true'
        )
    run_crawler = DockerOperator(
        task_id='run_tmdb_crawler',
        image='project_tlcn-tmdb-crawler:latest', 
        container_name='tmdb-crawler-Daily',
        api_version='auto',
        auto_remove=True,  
        docker_url='unix://var/run/docker.sock', 
        network_mode='project_tlcn_default',
        
    )


    spark_streaming = SparkSubmitOperator(
        task_id='streaming_process',
        application='/opt/airflow/jobs/python/processingAPI_1_toBronze.py',
        jars="/opt/airflow/jars/hadoop-aws-3.3.4.jar,"
             "/opt/airflow/jars/s3-2.18.41.jar,"
             "/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,"
             "/opt/airflow/jars/aws-java-sdk-1.12.367.jar,"
             "/opt/airflow/jars/delta-core_2.12-2.2.0.jar,"
             "/opt/airflow/jars/delta-storage-2.2.0.jar,"
             "/opt/airflow/jars/kafka-clients-3.3.2.jar,"
             "/opt/airflow/jars/commons-pool2-2.11.1.jar,"
             "/opt/airflow/jars/spark-token-provider-kafka-0-10_2.12-3.3.2.jar,"
             "/opt/airflow/jars/spark-sql-kafka-0-10_2.12-3.2.1.jar",
        conn_id="spark-conn",
        conf={
            "spark.executor.memory": "1G",  # Giới hạn RAM cho executor
            "spark.executor.cores": "2",  # Giới hạn số core
            "spark.dynamicAllocation.enabled": "false",  # Tắt phân bổ động (để kiểm soát tốt hơn)
            "spark.streaming.backpressure.enabled": "true",  # Giúp tránh quá tải dữ liệu streaming
            "spark.sql.shuffle.partitions": "10"  # Giới hạn số partition khi shuffle dữ liệu
        }
    )

    remove_existing_container >> run_crawler 
