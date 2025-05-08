import praw
from kafka import KafkaProducer
import json
import time
import os
import yaml
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

CONFIG_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'configuration', 'reddit_config.yml'))
try:
    with open(CONFIG_PATH, 'r') as f:
        reddit_config = yaml.safe_load(f)
    subreddits = reddit_config.get("subreddits", [])
    logger.info(f"Loaded subreddits: {subreddits}")
except Exception as e:
    logger.error(f"Failed to load subreddit config: {e}")
    subreddits = []

try:
    reddit = praw.Reddit(
        client_id=os.getenv("REDDIT_CLIENT_ID"),
        client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
        user_agent=os.getenv("REDDIT_USER_AGENT"),
        username=os.getenv("REDDIT_USERNAME"),
        password=os.getenv("REDDIT_PASSWORD"),
    )
    logger.info("Successfully authenticated with Reddit API.")
except Exception as e:
    logger.error(f"Reddit authentication failed: {e}")
    raise

try:
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logger.info("Kafka producer initialized.")
except Exception as e:
    logger.error(f"Failed to initialize Kafka producer: {e}")
    raise

seen_posts = set()  

def fetch_posts():
    for subreddit_name in subreddits:
        try:
            subreddit = reddit.subreddit(subreddit_name)
            logger.info(f"Fetching posts from r/{subreddit_name}...")

            for post in subreddit.new(limit=10):
                if post.id not in seen_posts:
                    post_data = {
                        "id": post.id,
                        "title": post.title,
                        "author": str(post.author),
                        "subreddit": subreddit_name,
                        "upvotes": post.score,
                        "url": post.url,
                        "created_utc": post.created_utc,
                        "text": post.selftext,
                        "is_self": post.is_self,
                        "num_comments": post.num_comments,
                    }

                    producer.send('reddit-posts', post_data)
                    seen_posts.add(post.id)
                    logger.info(f"Sent post to Kafka: {post.title[:50]}...")
                else:
                    logger.debug(f"Duplicate skipped: {post.title[:50]}...")
        except Exception as e:
            logger.warning(f"Error fetching from r/{subreddit_name}: {e}")
        time.sleep(2)

if __name__ == "__main__":
    logger.info("Starting Reddit fetch loop...")
    while True:
        fetch_posts()
        logger.info("Sleeping for 60 seconds...")
        time.sleep(60)
