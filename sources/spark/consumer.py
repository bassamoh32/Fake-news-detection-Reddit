import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import logging
logging.basicConfig(level=logging.INFO)

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS reddit
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")



def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS reddit.reddit_posts (
            id TEXT PRIMARY KEY,
            title TEXT,
            author TEXT,
            subreddit TEXT,
            upvotes INT,
            url TEXT,
            created_at TIMESTAMP,
            text TEXT
        );
    """)
    print("Table created successfully!")



def create_spark_connection():
    try:
        spark = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
            .config('spark.cassandra.connection.host', '192.168.1.6') \
            .config("spark.cassandra.connection.port", "9042") \
            .config("spark.cassandra.auth.username", "cassandra") \
            .config("spark.cassandra.auth.password", "cassandra") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        return spark
    except Exception as e:
        logging.error(f"Could not create Spark session: {e}")
        return None
    

def connect_to_kafka(spark_conn):
    try:
        df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', 'users_created') \
            .option("failOnDataLoss", "false") \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info(f"kafka dataframe created successfully {df}")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")
    return df
    

def create_cassandra_connection():
    try:
        cluster = Cluster(['192.168.1.6'])

        cas_session = cluster.connect()

        create_keyspace(cas_session)
        create_table(cas_session)

        return cas_session
    except Exception as e:
        import traceback
        logging.error(f"Could not create cassandra connection due to {e}")
        logging.error(traceback.format_exc())
        return None

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType()),
        StructField("title", StringType()),
        StructField("author", StringType()),
        StructField("subreddit", StringType()),
        StructField("upvotes", IntegerType()),
        StructField("url", StringType()),
        StructField("created_utc", FloatType()),
        StructField("text", StringType())
    ])
    return spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")


if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        session = create_cassandra_connection()

        if session:
            spark_df = connect_to_kafka(spark_conn)

            selection_df = create_selection_df_from_kafka(spark_df)
            
            spark_df.printSchema()

            logging.info("Streaming is being started...")
        

            try:
                streaming_query = selection_df.writeStream \
                                .format("org.apache.spark.sql.cassandra") \
                                .options(table="reddit_posts", keyspace="reddit") \
                                .option("checkpointLocation", "/tmp/checkpoint") \
                                .trigger(processingTime="5 seconds") \
                                .start()
    
                print(f"Streaming is being started 2...")

                print(streaming_query.status)
                print(f"Query Status: {streaming_query.status}")
                print(f"Recent Progress: {streaming_query.recentProgress}")

                if streaming_query.isActive:
                    print("Query is running")
                
                streaming_query.awaitTermination(100)

                print("Streaming is being started 3...")

            except Exception as e:
                print(f"Streaming query failed due to: {e}")