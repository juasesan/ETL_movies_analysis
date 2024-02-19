from libs import *

@dag(
    dag_id='movies_ETL',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2024,2,17),
    catchup=False
)

def etl_process():

    create_database = SqliteOperator(
        task_id='create_table_sqlite',
        sql=r"""
        CREATE TABLE IF NOT EXISTS now_playing(
            title TEXT PRIMARY KEY,
            release_date DATE,
            original_language CHAR(2),
            adult TEXT CHECK(adult IN ('True', 'False')),
            video TEXT CHECK(video IN ('True', 'False')),
            popularity FLOAT,
            vote_average FLOAT,
            vote_count INTEGER,
            genre_ids TEXT
        )
        """
    )