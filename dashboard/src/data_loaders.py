import pandas as pd
import streamlit as st
from .db import get_connection

@st.cache_data(ttl=3600)
def load_core_movies_data():
    conn = get_connection()
    df = pd.read_sql("""
        SELECT id, title, release_date, original_language, popularity, vote_average, vote_count
        FROM movies
    """, conn)
    conn.close()
    return df

@st.cache_data(ttl=3600)
def load_genre_distribution():
    conn = get_connection()
    df = pd.read_sql("""
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
    df = pd.read_sql("""
        SELECT original_language, COUNT(*) as count
        FROM movies
        GROUP BY original_language
        ORDER BY count DESC
        LIMIT 10;
    """, conn)
    conn.close()
    return df
