import os
import sys
from cassandra.cluster import Cluster
import pandas as pd
from transformers import RobertaTokenizer, RobertaForSequenceClassification
import torch.nn.functional as F
import torch
import time
import yaml
from datetime import datetime

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

model_path = os.path.join(os.path.dirname(__file__), "final_model")
if not os.path.exists(model_path):
    raise FileNotFoundError(f"Model directory not found at: {model_path}")

tokenizer = RobertaTokenizer.from_pretrained(model_path)
model = RobertaForSequenceClassification.from_pretrained(model_path)
model.eval()

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from cassandra_utils.cassandra_manager import CassandraManager

def load_config(config_file):
    config_path = os.path.join(os.path.dirname(__file__), '..', 'configuration', config_file)
    with open(config_path) as f:
        return yaml.safe_load(f)

cassandra_config = load_config('cassandra.yml')

CHECK_INTERVAL = 20
PROCESS_LIMIT = 50
CASSANDRA_HOSTS = cassandra_config['HOST']
KEYSPACE = cassandra_config['KEYSPACE']
TABLE = cassandra_config['TABLE']
USERNAME = cassandra_config['USERNAME']
PASSWORD = cassandra_config['PASSWORD']

cassandra = CassandraManager(
    host=CASSANDRA_HOSTS,
    keyspace=KEYSPACE,
    table=TABLE,
    username=USERNAME,
    password=PASSWORD
)
cassandra.connect()
session = cassandra.get_session()

update_stmt = session.prepare(f"""
    UPDATE {TABLE}
    SET prob_fake = ?,
        prediction = ?,
        last_checked = toTimestamp(now())
    WHERE post_date = ?
    AND post_time = ?
    AND post_id = ?
""")

def classify_text(text):
    inputs = tokenizer(text, return_tensors="pt", truncation=True, padding=True, max_length=512)
    with torch.no_grad():
        probs = F.softmax(model(**inputs).logits, dim=1)
    return probs[0][1].item()

def get_recent_posts_today():
    today = datetime.utcnow().date()
    rows = session.execute(f"""
        SELECT post_id, title, text, prob_fake, post_time 
        FROM {TABLE}
        WHERE post_date = %s
        LIMIT %s
    """, (today, PROCESS_LIMIT * 3))
    return pd.DataFrame(list(rows))

def process_posts():
    data = get_recent_posts_today()
    if data.empty:
        print("No recent posts found")
        return 0

    unclassified = data[data['prob_fake'].isin([None, -1.0])].head(PROCESS_LIMIT)
    if unclassified.empty:
        print("No unclassified posts in recent batch")
        return 0

    unclassified['combined_text'] = unclassified['title'].fillna('') + " " + unclassified['text'].fillna('')
    unclassified['prob_fake'] = unclassified['combined_text'].apply(classify_text)
    unclassified = unclassified.dropna(subset=['prob_fake'])
    unclassified['prediction'] = unclassified['prob_fake'].apply(lambda p: 'Fake' if p > 0.85 else 'True')

    today = datetime.utcnow().date()
    for _, row in unclassified.iterrows():
        try:
            post_time = row['post_time']
            if isinstance(post_time, str):
                post_time = datetime.strptime(post_time, "%Y-%m-%d %H:%M:%S")

            session.execute(update_stmt, (
                float(row.prob_fake),
                row.prediction,
                today,
                post_time,
                row.post_id
            ))
        except Exception as e:
            print(f"Error updating post {row.post_id}: {str(e)}")
            print(f"Post time value: {row['post_time']} (type: {type(row['post_time'])})")
            continue

    return len(unclassified)

def continuous_classification():
    print("Starting classification service for time-based table...")
    try:
        while True:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n[{now}] Processing...")

            processed = process_posts()
            print(f"Successfully classified {processed} posts")

            time.sleep(CHECK_INTERVAL)
    except KeyboardInterrupt:
        print("\nStopping service...")
    finally:
        cassandra.close()

if __name__ == "__main__":
    continuous_classification()
