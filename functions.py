import requests
import pandas as pd
import matplotlib.pyplot as plt
import sqlite3

from airflow.providers.sqlite.hooks.sqlite import SqliteHook

def extract_data_from_API(num_pages):
    headers = {
        "accept": "application/json",
    }

    with open("key.txt") as file: 
        headers['Authorization'] = file.readline()

    # Retrieving movies list
    movies = []
    for i in range(num_pages):
        url = f"https://api.themoviedb.org/3/movie/now_playing?language=en-US&page={i+1}"

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            response = response.json()
            movies += response['results']   

        else:
            print(f"Requesting movies failed in iteration {i+1} with status code: {response.status_code}")
    movies = pd.DataFrame(movies)
    
    # Retrieving genres list
    url_genres = "https://api.themoviedb.org/3/genre/movie/list?language=en"

    genres = requests.get(url_genres, headers=headers)

    if genres.status_code == 200:
        genres = genres.json()
        genres = pd.DataFrame(genres['genres'])
        genres.columns = ['genre_id', 'genre_name']

    else:
        print(f"Requesting genres failed with status code: {genres.status_code}")
    
    return movies, genres



def transform_data(movies_df):
    # Dropping duplicated by id
    movies_df.drop_duplicates(subset='id', inplace=True)

    # Create moves_genres dataframe pairing each genre_id with its movie id (many-to-many relationship)
    movies_genres = movies_df[['id', 'genre_ids']].explode('genre_ids')
    movies_genres.columns = ['movie_id', 'genre_id']

    # Removing unnecessary columns from movies_df
    movies_df.drop(columns=['genre_ids', 'backdrop_path', 'overview', 'original_title', 'poster_path'], inplace=True)
    
    # Transforming dates to datetime
    movies_df['release_date'] = pd.to_datetime(movies_df['release_date'])

    # Reorder columns
    columns_ordered = ['adult', 'id', 'original_language', 'popularity', 'release_date', 'title', 'video', 'vote_average', 'vote_count']
    movies_df = movies_df.reindex(columns=columns_ordered)

    return movies_df, movies_genres


def load_data_to_db(movies_df, genres_df, movies_genres):
    # Stablish connection to movies.db
    hook = SqliteHook(sqlite_conn_id = 'movies')
    engine = hook.get_sqlalchemy_engine()

    # Check whether movies are already in the db or not
    stored_movies = hook.get_pandas_df('SELECT * FROM movies;')
    for movie_index, movie_row in movies_df.iterrows():
        if movie_row['id'] in stored_movies['id'].values:
            # Drop movie from movies_df and movies_genres
            movies_df.drop(index = movie_index, inplace=True)
            movies_genres.drop(index = movie_index, inplace=True)

    # Check whether genres are already in the db or not
    stored_genres = hook.get_pandas_df('SELECT * FROM genres;')
    for genre_index, genre_row in genres_df.iterrows():
        if genre_row['genre_id'] in stored_genres['genre_id'].values:
            genres_df.drop(index = genre_index, inplace=True)

    # Load new content to each table
    movies_df.to_sql(name='movies', con=engine, if_exists='append', index=False, method='multi')
    genres_df.to_sql(name='genres', con=engine, if_exists='append', index=False, method='multi')
    movies_genres.to_sql(name='movies_genres', con=engine, if_exists='append', index=False, method='multi')


def query_sqlite_database(sql_query, database_path):
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
        return df
    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of an error
    finally:
        # Ensure the database connection is closed
        conn.close()