from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy.exc import ProgrammingError

def create_db_connection(url):
    """Create and return a database connection."""
    engine = create_engine(url)
    return engine



def insert_dataframe_to_db(df, table_name, engine, index=False):
    """Insert a DataFrame into a database table, with a fallback strategy."""
    try:
        # First try to append
        df.to_sql(table_name, engine, if_exists='append', index=index)
    except ProgrammingError as e:
        # If append fails, replace the table
        print("Append failed, replacing table:", e)
        df.to_sql(table_name, engine, if_exists='replace', index=index)
