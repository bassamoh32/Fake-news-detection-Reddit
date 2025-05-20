from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
import logging

# Logger configuration
logger = logging.getLogger('reddit_pipeline')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

default_args = {
    'owner': 'reddit_airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'reddit_data_pipeline',
    default_args=default_args,
    description='Process Reddit posts through Kafka (port 19092), Spark, and ML classification',
    schedule_interval='@daily',
    start_date=datetime(2025, 5, 19),
    catchup=False,
    tags=['reddit', 'kafka', 'spark'],
) as dag:

    # Start/End markers
    start_task = EmptyOperator(task_id='start_pipeline')
    end_task = EmptyOperator(task_id='end_pipeline')

    # Kafka Producer Task (using port 19092)
    run_producer = DockerOperator(
        task_id='run_kafka_producer',
        image='kafka-producer:latest',
        container_name='kafka-producer',
        command='python /app/kafka/producer.py',
        auto_remove=False,
        docker_url='unix:///var/run/docker.sock',
        network_mode='RedditPipeline',
        environment={
            'KAFKA_BROKERS': 'kafka:19092',  
            'PYTHONPATH': '/app',
            'REDDIT_CLIENT_ID': '{{ var.value.reddit_client_id }}',
            'REDDIT_CLIENT_SECRET': '{{ var.value.reddit_client_secret }}'
        },
        mem_limit='512m',
        retries=2
    )

    # Spark Streaming Task
    start_streaming = DockerOperator(
        task_id='run_spark_streaming',
        image='reddit_pipeline-spark-streaming:latest',
        container_name='spark-streaming',
        command=[
            "spark-submit",
            "--master", "spark://spark-master:7077",
            "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0",
            "--conf", "spark.cassandra.connection.host=cassandra",
            "--conf", "spark.cassandra.connection.port=9042",
            "--conf", "spark.cassandra.auth.username=admin",
            "--conf", "spark.cassandra.auth.password=admin",
            "--conf", "spark.executor.memory=2g",
            "/app/consumer.py"
        ],
        auto_remove=False,
        docker_url='unix:///var/run/docker.sock',
        network_mode='RedditPipeline',
        environment={
            'PYTHONPATH': '/app',
            'SPARK_HOME': '/opt/bitnami/spark',
            'KAFKA_BOOTSTRAP_SERVERS': 'kafka:19092'  # Also here if Spark consumes from Kafka
        },
        mem_limit='4g'
    )

    # Classification Task
    classification_task = DockerOperator(
        task_id='run_classification',
        image='classification-model:latest',
        command="python /app/model/classification.py",
        auto_remove=True,
        docker_url='unix:///var/run/docker.sock',
        network_mode='RedditPipeline',
        mem_limit='2g'
    )

    # Task dependencies
    start_task >> run_producer >> start_streaming >> classification_task >> end_task