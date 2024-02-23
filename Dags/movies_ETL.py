import sys
sys.path.append('/home/juasesan/python_projects/ETL_movies_analysis')

from libs import *
from etl_functions import *


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

    @task()
    def etl():
        data_df = extract_data_from_API(num_pages=10)
        data_df = transform_data(data_df)
        return data_df
    
    movies_list = etl()
    create_database.set_downstream(movies_list)


result = etl_process()