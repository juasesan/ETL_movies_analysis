# Movies Database ETL pipeline and analysis
This repository contains the code for an ETL (Extract, Transform, Load) pipeline built with Python and Apache Airflow to process data from The Movies Database and store it in a SQLite Data Warehouse for further Exploratory Data Analysis (EDA) and machine learning modeling.

## Overview
The Movies Database ETL pipeline extracts data of now playing movies from TMDB, cleans and transforms it into a suitable format, and loads it into each SQLite table previously created. The general Airflow DAG is presented:

<div align="center">
    
 ![image](https://github.com/juasesan/ETL_movies_analysis/assets/51239155/ccc4ce71-b3df-4cc5-9a78-1f71eb03380e)

</div>

Where `etl()` is defined as:
```
@task()
def etl():
    # Extract
    movies_df, genres_df = extract_data_from_API(num_pages=100)

    # Transform
    movies_df, movies_genres = transform_data(movies_df)

    # Load
    load_data_to_db(movies_df, genres_df, movies_genres)
```

And the final data warehouse squema is:

<div align="center">
    
 ![diagrama_sql_etl](https://github.com/juasesan/ETL_movies_analysis/assets/51239155/fd871657-09a5-486f-a439-d2d6e7c7b10c)

</div>

## Features
- Automated Workflow: Utilizes Apache Airflow for orchestrating the ETL process, ensuring scheduled and automated execution.
- Modular Design: The pipeline is designed with modularity in mind, allowing for easy addition or modification of extraction, transformation, and loading tasks.
- Data Quality Checks: Includes data quality checks at various stages of the pipeline to ensure accuracy and completeness.
- Simple design: Intuitive step-by-step process which can be easily read.

## Requirements
- Python 3.x
- Apache Airflow
- SQLite
- Additional Python dependencies listed in requirements.txt

## Future work
Migrate this solution to AWS, leveraging technologies such as:
- Lambda: For API calling and storing data in S3 buckets for next steps.
- Glue: Where data transformations would be performed.
- RDS: As a Data Warehouse powered by PostgreSQL where data is loaded for analysis, replacing SQLite.
- Step Functions: For orchestrating each step of the pipeline, replacing Apache Airflow.

## Contributing
Contributions are welcome! If you have any suggestions, bug fixes, or improvements, feel free to open an issue or create a pull request.

## Acknowledgments
[The Movies Database](https://developer.themoviedb.org/docs/getting-started) for providing the dataset used in this project.
Apache Airflow community for developing a powerful workflow management platform.
Python community for creating an extensive ecosystem of libraries and tools.
