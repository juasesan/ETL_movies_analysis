# src/etl/database.py
import os
import pandas as pd
from typing import List, Dict, Any
from sqlalchemy import create_engine
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from dotenv import load_dotenv
from models import Movie, Genre, MovieGenre

class MovieDatabase:
    """Handler for database operations with movie data."""
    
    def __init__(self, connection_id: str = 'movies'):
        """
        Initialize database connection.
        
        Args:
            connection_id: Airflow connection ID for SQLite database
        """
        load_dotenv()
        self.hook = SqliteHook(sqlite_conn_id=connection_id)
        self.engine = self.hook.get_sqlalchemy_engine()
        
    
    def load_movies(self, movies: List[Movie]) -> None:
        """
        Load movies into the database, skipping existing ones by title.
        
        Args:
            movies: List of Movie instances to load
        """
        # Get existing movie titles
        existing_titles = set(self.hook.get_pandas_df('SELECT title FROM movies;')['title'].values)
        
        # Filter out existing movies by title
        new_movies = [movie for movie in movies if movie.title not in existing_titles]
        
        if not new_movies:
            print("No new movies to load.")
            return
            
        # Convert to DataFrame and load
        movies_df = pd.DataFrame([movie.to_dict() for movie in new_movies])
        with self.hook.get_conn() as conn:
            movies_df.to_sql(name='movies', con=conn, if_exists='append', 
                         index=False, method='multi')
        print(f"Loaded {len(new_movies)} new movies.")
    
    def load_genres(self, genres: List[Genre]) -> None:
        """
        Load genres into the database, skipping existing ones.
        
        Args:
            genres: List of Genre instances to load
        """
        # Get existing genre IDs
        existing_ids = set(self.hook.get_pandas_df('SELECT genre_id FROM genres;')['genre_id'].values)
        
        # Filter out existing genres
        new_genres = [genre for genre in genres if genre.genre_id not in existing_ids]
        
        if not new_genres:
            print("No new genres to load.")
            return
            
        # Convert to DataFrame and load
        genres_df = pd.DataFrame([{'genre_id': g.genre_id, 'genre_name': g.genre_name} 
                                 for g in new_genres])
        with self.hook.get_conn() as conn:
            genres_df.to_sql(name='genres', con=conn, if_exists='append', 
                         index=False, method='multi')
        print(f"Loaded {len(new_genres)} new genres.")
    
    def load_movie_genres(self, movie_genres: List[MovieGenre]) -> None:
        """
        Load movie-genre relationships into the database.
        
        Args:
            movie_genres: List of MovieGenre instances to load
        """
        if not movie_genres:
            print("No movie-genre relationships to load.")
            return
            
        # Convert to DataFrame and load
        mg_df = pd.DataFrame([{'movie_id': mg.movie_id, 'genre_id': mg.genre_id} 
                             for mg in movie_genres])
        with self.hook.get_conn() as conn:
            mg_df.to_sql(name='movies_genres', con=conn, if_exists='append', 
                     index=False, method='multi')
        print(f"Loaded {len(movie_genres)} movie-genre relationships.")