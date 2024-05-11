import psycopg2
from psycopg2 import OperationalError
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    database_hostname: str = "POSTGRES_HOSTNAME"
    database_port: str = "POSTGRES_PORT"
    database_password: str = "POSTGRES_PASSWORD"
    database_name: str = "POSTGRES_DB"
    database_username: str = "POSTGRES_USER"

    class Config:
        env_file = ".env_config"

settings = Settings()

def create_connection():
    try:
        conn = psycopg2.connect(
            host=settings.database_hostname,
            user=settings.database_username,
            password=settings.database_password,
            dbname=settings.database_name,
            port=settings.database_port
        )
        return conn
    except OperationalError as e:
        print(f"The error '{e}' occurred")
        return None

def create_test_table(conn):
    """Create a test table if it doesn't already exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS test (
        id SERIAL PRIMARY KEY,
        message TEXT NOT NULL
    )
    """
    cursor = conn.cursor()
    try:
        cursor.execute(create_table_query)
        conn.commit()
        print("Query executed successfully: test table created or verified.")
    except OperationalError as e:
        print(f"The error '{e}' occurred")

def insert_into_test_table(conn, message):
    """Insert a message into the test table."""
    insert_query = """
    INSERT INTO test (message) VALUES (%s)
    """
    cursor = conn.cursor()
    try:
        cursor.execute(insert_query, (message,))
        conn.commit()
        print("Query executed successfully: Message inserted into test table.")
    except OperationalError as e:
        print(f"The error '{e}' occurred")

if __name__ == "__main__":
    # Connect to the database
    conn = create_connection()
    
    if conn is not None:
        # Create a test table
        create_test_table(conn)
        
        # Insert a message into the test table
        insert_into_test_table(conn, "Hello, this is a test message.")
