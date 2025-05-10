import os
import sys
import logging
from typing import Dict, Any
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

CASSANDRA_CONFIG = {
    "host": "cassandra",
    "port": 9042,
    "username": "admin",
    "password": "admin"
}

KAFKA_CONFIG = {
    "bootstrap_servers": "kafka:19092",
    "topic": "reddit-posts",
    "starting_offsets": "latest"
}

REDDIT_SCHEMA = StructType([
    StructField("id", StringType(), nullable=False),
    StructField("title", StringType()),
    StructField("author", StringType()),
    StructField("subreddit", StringType()),
    StructField("upvotes", IntegerType()),
    StructField("url", StringType()),
    StructField("created_utc", FloatType()),
    StructField("text", StringType())
])

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('consumer.log')
    ]
)
logger = logging.getLogger(__name__)

def create_spark_session() -> SparkSession:
    
    return SparkSession.builder \
        .appName("RedditStreamProcessor") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1",
            "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0"
        ])) \
        .config("spark.cassandra.connection.host", CASSANDRA_CONFIG["host"]) \
        .config("spark.cassandra.connection.port", CASSANDRA_CONFIG["port"]) \
        .config("spark.cassandra.auth.username", CASSANDRA_CONFIG["username"]) \
        .config("spark.cassandra.auth.password", CASSANDRA_CONFIG["password"]) \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    try:
        processed_df = batch_df.withColumn(
            "created_at", 
            from_unixtime(col("created_utc"))
        ).drop("created_utc")
        
        (processed_df.write
         .format("org.apache.spark.sql.cassandra")
         .mode("append")
         .options(
             table="reddit_posts", 
             keyspace="reddit",
             ttl="864000"  
         )
         .save())
        
        logger.info(f"Processed batch {batch_id} with {processed_df.count()} records")
    except Exception as e:
        logger.error(f"Failed batch {batch_id}: {str(e)}")

def initialize_cassandra():
    from cassandra_manager import CassandraManager
    from utils.schema import CassandraSchema
    
    with CassandraManager(**CASSANDRA_CONFIG) as session:
        CassandraSchema.create_keyspace(session)
        CassandraSchema.create_table(session)
        logger.info("Cassandra schema initialized")

def start_streaming():
    try:
        initialize_cassandra()
        spark = create_spark_session()
        
        df = (spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", KAFKA_CONFIG["bootstrap_servers"])
              .option("subscribe", KAFKA_CONFIG["topic"])
              .option("startingOffsets", KAFKA_CONFIG["starting_offsets"])
              .option("failOnDataLoss", "false")
              .load())
        
        query = (df.select(from_json(col("value").cast("string"), REDDIT_SCHEMA).alias("data"))
                .select("data.*")
                .writeStream
                .foreachBatch(process_batch)
                .outputMode("append")
                .option("truncate", "false")
                .start())
        
        logger.info("Streaming pipeline started")
        query.awaitTermination()
        
    except Exception as e:
        logger.critical(f"Pipeline failed: {e}", exc_info=True)
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    start_streaming()