import pandas as pd
import streamlit as st
from .db import get_connection, query_db

@st.cache_data(ttl=3600)
def load_core_movies_data():
    conn = get_connection()
    df = query_db("""
        SELECT title, release_date, original_language, popularity, overview, vote_average, vote_count
        FROM movies
    """, conn)
    conn.close()
    return df

@st.cache_data(ttl=3600)
def load_genres():
    conn = get_connection()
    df = query_db("""
        SELECT genre_name
        FROM genres
    """, conn)
    conn.close()
    return list(df['genre_name'])

@st.cache_data(ttl=3600)
def load_genre_distribution():
    conn = get_connection()
    df = query_db("""
        SELECT g.genre_name, COUNT(*) as count 
        FROM genres g
        JOIN movies_genres mg ON g.genre_id = mg.genre_id
        GROUP BY g.genre_name
        ORDER BY count DESC
    """, conn)
    conn.close()
    return df

@st.cache_data(ttl=3600)
def load_language_distribution():
    conn = get_connection()
    df = query_db("""
        SELECT original_language, COUNT(*) as count
        FROM movies
        GROUP BY original_language
        ORDER BY count DESC
        LIMIT 10;
    """, conn)
    conn.close()
    return df

@st.cache_data(ttl=3600)
def load_popularity_by_genre(genre_name):
    conn = get_connection()
    df = query_db(f"""
        SELECT m.title, m.release_date, m.overview
        FROM movies as m
        JOIN movies_genres AS mg
        ON m.id = mg.movie_id
        LEFT JOIN genres as g
        ON mg.genre_id = g.genre_id
        WHERE g.genre_name = "{genre_name}"
        LIMIT 10
    """, conn)
    conn.close()
    return df