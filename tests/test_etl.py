import pytest
from src.etl.functions import extract_data_from_API, transform_data, load_data_to_db

def test_extract_data():
    """Test that data extraction returns expected dataframes."""
    movies_df, genres_df = extract_data_from_API(num_pages=1)
    assert movies_df is not None
    assert genres_df is not None
    assert not movies_df.empty
    assert not genres_df.empty

def test_transform_data():
    """Test that data transformation works correctly."""
    movies_df, genres_df = extract_data_from_API(num_pages=1)
    transformed_movies, movies_genres = transform_data(movies_df)
    assert transformed_movies is not None
    assert movies_genres is not None
    assert not transformed_movies.empty
    assert not movies_genres.empty

def test_load_data():
    """Test that data loading works correctly."""
    movies_df, genres_df = extract_data_from_API(num_pages=1)
    transformed_movies, movies_genres = transform_data(movies_df)
    load_data_to_db(transformed_movies, genres_df, movies_genres)
    # Add assertions to verify data was loaded correctly 