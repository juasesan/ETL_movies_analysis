from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import pendulum

from functions import *


@dag(
    dag_id="movies_etl_process",
    schedule="@daily",
    start_date=pendulum.datetime(2024,2,24),
    catchup=False
)
def movies_etl_process():

    create_movies_table = SQLExecuteQueryOperator(
        task_id='create_movies_table',
        conn_id='movies',
        sql=r"""
        CREATE TABLE IF NOT EXISTS movies(
            id INTEGER NOT NULL PRIMARY KEY,
            title TEXT NOT NULL,
            release_date DATE,
            original_language CHAR(2),
            adult INTEGER CHECK (adult IN (0, 1)),
            video INTEGER CHECK (video IN (0, 1)),
            popularity FLOAT,
            overview TEXT,
            vote_average FLOAT,
            vote_count INTEGER
        );
        """
    )

    create_genres_table = SQLExecuteQueryOperator(
        task_id='create_genres_table',
        conn_id='movies',
        sql=r"""
        CREATE TABLE IF NOT EXISTS genres(
            genre_id INTEGER PRIMARY KEY,
            genre_name TEXT
        );
        """
    )

    create_movies_genres_table = SQLExecuteQueryOperator(
        task_id='create_movies_genres_table',
        conn_id='movies',
        sql=r"""
        CREATE TABLE IF NOT EXISTS movies_genres(
            movie_id INTEGER,
            genre_id INTEGER,
            PRIMARY KEY (movie_id, genre_id),
            FOREIGN KEY (movie_id) REFERENCES Movies(movie_id),
            FOREIGN KEY (genre_id) REFERENCES Genres(genre_id)
        );
        """
    )

    @task()
    def etl():
        # Extract
        movies_df, genres_df = extract_data_from_API()

        # Transform
        movies_df, movies_genres = transform_data(movies_df)

        # Load
        load_data_to_db(movies_df, genres_df, movies_genres)

    
    movies_list = etl()
    create_movies_table.set_downstream(create_genres_table)
    create_genres_table.set_downstream(create_movies_genres_table)
    create_movies_genres_table.set_downstream(movies_list)


movies_etl_process()