from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pendulum

from etl_functions import *


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
            title TEXT,
            release_date DATE,
            original_language CHAR(2),
            adult TEXT CHECK(adult IN ('True', 'False')),
            video TEXT CHECK(video IN ('True', 'False')),
            popularity FLOAT,
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
        movies_df, genres_df = extract_data_from_API(num_pages=10)
        movies_df = transform_data(movies_df)
        return movies_df
    
    movies_list = etl()
    create_movies_table.set_downstream(create_genres_table)
    create_genres_table.set_downstream(create_movies_genres_table)
    create_movies_genres_table.set_downstream(movies_list)


movies_etl_process()