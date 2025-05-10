import os
import time
import json
import yaml
import logging
from typing import Dict, Any
from dotenv import load_dotenv
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
import praw

KAFKA_CONFIG = {
    "bootstrap_servers": "kafka:19092",
    "topic": "reddit-posts",
    "client_id": "reddit-producer-v1",
    "message_timeout": 30000,  
    "retries": 5
}

REDDIT_CONFIG_PATH = os.path.join(
    os.path.dirname(__file__), 
    '..', 
    'configuration', 
    'reddit.yml'
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('producer.log')
    ]
)
logger = logging.getLogger(__name__)

def load_config() -> Dict[str, Any]:
    try:
        with open(REDDIT_CONFIG_PATH, 'r') as f:
            config = yaml.safe_load(f)
            logger.info(f"Loaded configuration for {len(config.get('subreddits', []))} subreddits")
            return config
    except Exception as e:
        logger.error(f"Config load failed: {e}")
        raise

def create_reddit_client() -> praw.Reddit:
    return praw.Reddit(
        client_id=os.getenv("REDDIT_CLIENT_ID"),
        client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
        user_agent=os.getenv("REDDIT_USER_AGENT"),
        timeout=15,
        retry_on_error=True,
        check_for_async=False
    )

def initialize_kafka() -> KafkaProducer:
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],
            client_id=KAFKA_CONFIG["client_id"],
            request_timeout_ms=10000
        )
        
        if KAFKA_CONFIG["topic"] not in admin.list_topics():
            admin.create_topics([NewTopic(
                name=KAFKA_CONFIG["topic"],
                num_partitions=1,
                replication_factor=1,
                topic_configs={
                    'retention.ms': '604800000', 
                    'cleanup.policy': 'compact,delete',
                    'segment.ms': '3600000'  
                }
            )])
            logger.info(f"Created Kafka topic: {KAFKA_CONFIG['topic']}")
            
        return KafkaProducer(
            bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=KAFKA_CONFIG["retries"],
            linger_ms=500,
            compression_type='gzip',
            request_timeout_ms=KAFKA_CONFIG["message_timeout"],
            max_block_ms=60000
        )
        
    except Exception as e:
        logger.error(f"Kafka initialization failed: {e}")
        raise

def process_post(post: praw.models.Submission) -> Dict[str, Any]:
    return {
        "id": post.id,
        "title": post.title,
        "author": str(post.author),
        "subreddit": post.subreddit.display_name,
        "upvotes": post.score,
        "url": post.url,
        "created_utc": post.created_utc,
        "text": post.selftext,
        "num_comments": post.num_comments,
        "collected_at": time.time(),
        "is_self": post.is_self
    }

def run_producer():
    load_dotenv()
    config = load_config()
    reddit = create_reddit_client()
    producer = initialize_kafka()
    seen_posts = set()
    cycle_count = 0
    
    try:
        while True:
            cycle_count += 1
            processed_count = 0
            start_time = time.time()
            
            for subreddit in config.get("subreddits", []):
                try:
                    for post in reddit.subreddit(subreddit).new(limit=20):
                        if post.id not in seen_posts:
                            message = process_post(post)
                            producer.send(
                                KAFKA_CONFIG["topic"],
                                value=message
                            )
                            seen_posts.add(post.id)
                            processed_count += 1
                            logger.debug(f"Sent: {post.title[:50]}...")
                            
                except Exception as e:
                    logger.error(f"Subreddit {subreddit} error: {e}")
                    time.sleep(5)  
            
            duration = time.time() - start_time
            logger.info(
                f"Cycle {cycle_count} complete. "
                f"Processed {processed_count} posts. "
                f"Total tracked: {len(seen_posts)}. "
                f"Duration: {duration:.2f}s"
            )
            
            sleep_time = max(30 - duration, 5)
            time.sleep(sleep_time)
            
    except KeyboardInterrupt:
        logger.info("Shutdown signal received")
    except Exception as e:
        logger.critical(f"Unexpected error: {e}", exc_info=True)
    finally:
        producer.flush()
        producer.close()
        logger.info("Kafka producer closed gracefully")

if __name__ == "__main__":
    run_producer()