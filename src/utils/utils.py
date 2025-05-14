import sqlite3
import pandas as pd


def query_db(sql_query, database_path):
    """
    Executes an SQL query on a SQLite database and returns the results as a pandas DataFrame.

    Parameters:
    - sql_query: The SQL query string to be executed.
    - database_path: The path to the SQLite database file.

    Returns:
    - A pandas DataFrame containing the results of the SQL query.
    """

    # Connect to the SQLite database
    conn = sqlite3.connect(database_path)

    try:
        # Execute the query and return the results in a DataFrame
        df = pd.read_sql_query(sql_query, conn)
        if ("release_date" in df.columns):
            df['release_date'] = pd.to_datetime(df['release_date'])

        return df
    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of an error
    finally:
        # Ensure the database connection is closed
        conn.close()