class CassandraSchema:
    @staticmethod
    def create_keyspace(session):
        session.execute("""
        CREATE KEYSPACE IF NOT EXISTS reddit
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """)

    @staticmethod
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