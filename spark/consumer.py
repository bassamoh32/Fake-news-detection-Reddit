import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime
from pyspark.sql.types import StructType, StringType, FloatType, IntegerType, BooleanType, TimestampType

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import CassandraManager
from cassandra_manager import CassandraManager

def load_config(config_file):
    """Load YAML configuration file from ./configuration/ directory"""
    config_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'configuration',
        config_file
    )
    try:
        import yaml
        with open(config_path) as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"Failed to load config {config_file}: {str(e)}")
        raise

# Load configurations
kafka_config = load_config('kafka.yml')
cassandra_config = load_config('cassandra.yml')

# Initialize Cassandra connection
cassandra = CassandraManager(
    host=cassandra_config['HOST'],
    keyspace=cassandra_config['KEYSPACE'],
    table=cassandra_config['TABLE']
)
cassandra.connect()

def process_batch(df, batch_id):
    """Process each batch of data and write to Cassandra"""
    if not df.isEmpty():
        print(f"Processing batch {batch_id} with {df.count()} records")

        try:
            # Select only required columns
            final_df = df.select("id", "timestamp", "subreddit", "text", "title") \
                         .filter(col("id").isNotNull() & col("timestamp").isNotNull())

            # Write to Cassandra
            final_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .options(
                    table=cassandra_config['TABLE'],
                    keyspace=cassandra_config['KEYSPACE']
                ) \
                .mode("append") \
                .save()

            print(f"Successfully wrote batch {batch_id} to Cassandra")
        except Exception as e:
            print(f"Error writing to Cassandra: {str(e)}")
    else:
        print(f"Batch {batch_id} is empty, skipping write")

def start_spark():
    """Start Spark Streaming to consume Reddit data from Kafka."""
    spark = SparkSession.builder \
        .appName("RedditStreamProcessor") \
        .config("spark.jars.packages", ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1",
            "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0"
        ])) \
        .config("spark.cassandra.connection.host", cassandra_config['HOST']) \
        .config("spark.cassandra.auth.username", cassandra_config.get('USERNAME', '')) \
        .config("spark.cassandra.auth.password", cassandra_config.get('PASSWORD', '')) \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
        .getOrCreate()

    # Define schema for Reddit data
    schema = StructType() \
        .add("id", StringType()) \
        .add("title", StringType()) \
        .add("author", StringType()) \
        .add("subreddit", StringType()) \
        .add("upvotes", IntegerType()) \
        .add("url", StringType()) \
        .add("created_utc", FloatType()) \
        .add("text", StringType()) \
        .add("is_self", BooleanType())

    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_config['BROKER']) \
        .option("subscribe", kafka_config['TOPIC']) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse the JSON data
    json_df = df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*")

    # Add timestamp column
    processed_df = json_df.withColumn(
        "timestamp",
        from_unixtime(col("created_utc")).cast(TimestampType())
    )

    # Write to Cassandra
    query = processed_df.writeStream \
        .foreachBatch(process_batch) \
        .start()

    print("Spark streaming started. Waiting for data from Kafka...")
    query.awaitTermination()

if __name__ == "__main__":
    try:
        start_spark()
    except Exception as e:
        print(f"Application error: {str(e)}")
    finally:
        if 'cassandra' in locals():
            cassandra.close()
