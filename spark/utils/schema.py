class CassandraSchema:
    CREATE_KEYSPACE = """
        CREATE KEYSPACE IF NOT EXISTS {0}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}};
    """

    CREATE_TABLE = """
        CREATE TABLE IF NOT EXISTS {0} (
            id TEXT,
            title TEXT,
            subreddit TEXT,
            text TEXT,
            timestamp TIMESTAMP,
            PRIMARY KEY ((id), timestamp)
        );
    """

    @staticmethod
    def get_insert_query(table: str):
        return f"INSERT INTO {table} (id, title, subreddit, text, timestamp) VALUES (%s, %s, %s, %s, %s);"
