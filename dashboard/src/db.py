import sqlite3
import pandas as pd

def get_connection():
    return sqlite3.connect("data/movies.db")

def query_db(sql_query, conn):
    """
    Executes an SQL query on a SQLite database and returns the results as a pandas DataFrame.

    Parameters:
    - sql_query: The SQL query string to be executed.
    - conn: Connection to the SQLite database.

    Returns:
    - A pandas DataFrame containing the results of the SQL query.
    """
    try:
        # Execute the query and return the results in a DataFrame
        df = pd.read_sql_query(sql_query, conn)
        if ("release_date" in df.columns):
            df['release_date'] = pd.to_datetime(df['release_date'])

        return df
    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of an error
