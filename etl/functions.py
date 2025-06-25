# src/etl/functions.py
import os
import pandas as pd
from typing import Tuple, List, Dict, Any
from api_client import TMDBApiClient
from models import Movie, Genre, MovieGenre
from database import MovieDatabase

def extract_data_from_API() -> Tuple[List[Movie], List[Genre]]:
    """
    Extract movie and genre data from TMDB API.
    
    Args:
        num_pages: Number of pages to fetch
        
    Returns:
        Tuple containing list of Movie and Genre objects
    """
    client = TMDBApiClient()
    
    # Get movies
    movies_data = client.get_now_playing_movies()
    movies = [Movie.from_dict(data) for data in movies_data]
    
    # Get genres
    genres_data = client.get_movie_genres()
    genres = [Genre.from_dict(data) for data in genres_data]
    
    return movies, genres

def transform_data(movies: List[Movie]) -> Tuple[List[Movie], List[MovieGenre]]:
    """
    Transform movie data and extract movie-genre relationships.
    
    Args:
        movies: List of Movie objects with genre_ids
        
    Returns:
        Tuple containing:
            - List of transformed Movie objects
            - List of MovieGenre relationship objects
    """
    # Extract movie-genre relationships
    movie_genres = []
    for movie in movies:
        movie_genres.extend(MovieGenre.from_movie(movie))
        
    return movies, movie_genres

def load_data_to_db(movies: List[Movie], genres: List[Genre], 
                   movie_genres: List[MovieGenre]) -> None:
    """
    Load data into the database.
    
    Args:
        movies: List of Movie objects
        genres: List of Genre objects
        movie_genres: List of MovieGenre relationship objects
    """
    db = MovieDatabase()
    db.load_movies(movies)
    db.load_genres(genres)
    db.load_movie_genres(movie_genres)

def etl_process(num_pages: int = 10) -> None:
    """
    Execute the complete ETL process.
    
    Args:
        num_pages: Number of pages to fetch from the API
    """
    # Extract
    movies, genres = extract_data_from_API(num_pages)
    
    # Transform
    transformed_movies, movie_genres = transform_data(movies)
    
    # Load
    load_data_to_db(transformed_movies, genres, movie_genres)
    
    print(f"ETL process completed successfully. Processed {len(transformed_movies)} movies.")

# Keep the query_db function for backward compatibility
def query_db(sql_query, database_path):
    """
    Executes an SQL query on a SQLite database and returns the results as a pandas DataFrame.

    Parameters:
    - sql_query: The SQL query string to be executed.
    - database_path: The path to the SQLite database file.

    Returns:
    - A pandas DataFrame containing the results of the SQL query.
    """
    import sqlite3
    
    # Connect to the SQLite database
    conn = sqlite3.connect(database_path)

    try:
        # Execute the query and return the results in a DataFrame
        df = pd.read_sql_query(sql_query, conn)
        if "release_date" in df.columns:
            df['release_date'] = pd.to_datetime(df['release_date'])

        return df
    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of an error
    finally:
        # Ensure the database connection is closed
        conn.close()

