import pandas as pd
import sqlite3
import os

def load_data():
    DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'movies.db')
    conn = sqlite3.connect(DB_PATH)
    # Try to infer the table name (assuming 'movies' or first table)
    query = "SELECT name FROM sqlite_master WHERE type='table';"
    tables = pd.read_sql(query, conn)
    table_name = tables.iloc[0, 0] if not tables.empty else None
    if table_name:
        df = pd.read_sql(f'SELECT * FROM {table_name}', conn)
    else:
        df = pd.DataFrame()
    conn.close()
    return df
