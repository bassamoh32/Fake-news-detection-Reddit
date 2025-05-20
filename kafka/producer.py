import os
import sys
import praw
from kafka import KafkaProducer
import json
import time
import yaml
from dotenv import load_dotenv

load_dotenv()

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

def load_config(config_file):
    config_path = os.path.join(os.path.dirname(__file__), '..', 'configuration', config_file)
    with open(config_path) as f:
        return yaml.safe_load(f)

reddit_config = load_config('reddit.yml')
kafka_config = load_config('kafka.yml')

reddit = praw.Reddit(
    client_id=os.getenv('REDDIT_CLIENT_ID'),
    client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
    user_agent=os.getenv('REDDIT_USER_AGENT')
)

producer = KafkaProducer(
    bootstrap_servers=kafka_config['BROKER'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=tuple(kafka_config['API_VERSION'])
)

seen_posts = set()

def fetch_posts():
    for subreddit_name in reddit_config['subreddits']:
        subreddit = reddit.subreddit(subreddit_name)
        print(f"Fetching posts from r/{subreddit_name}...")
        
        for post in subreddit.new(limit=reddit_config.get('POST_LIMIT', 10)):
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
                }
                
                producer.send(kafka_config['TOPIC'], post_data)
                seen_posts.add(post.id)
                print(f"✅ New post sent to Kafka: {post.title[:50]}...")
            else:
                print(f"⏭️ Skipping duplicate post: {post.title[:50]}...")
        
        time.sleep(reddit_config.get('SUBREDDIT_DELAY', 2))
          
if __name__ == "__main__":
    try:
        while True:
            fetch_posts()
            time.sleep(reddit_config.get('FETCH_INTERVAL', 60))
    except KeyboardInterrupt:
        print("\nShutting down...")
        producer.close()