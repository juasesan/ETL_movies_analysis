import streamlit as st
import plotly.express as px
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from src.data_loaders import *
from src.visualizations import *

# Set page config
st.set_page_config(
    page_title="Movies Dashboard",
    page_icon="🎬",
    layout="wide"
)

st.title("Want to know what's on movie theaters now? Check this dashboard")
st.markdown(
"""
* Here you can vizualize movies data obtained from The Movie DataBase (TMDB) 🎬
* TMDB provides an API for getting a list of movies that are currently on theatres all around the world 🌎
* To explore the back-end services that power this Dashboard, check out [this repository](https://github.com/juasesan/ETL_movies_analysis) 💻
* Some terminology for your reference:
    - Release Date: The original release date of the movie (is common to see some re-launched movies, like: The Lord of the Rings, Star Wars, etc)
    - Vote Average: The rating from 1 to 10 of the movie
    - Vote Count: How many people have voted to rate the movie
""")

# Main dashboard
def main():
    df = load_core_movies_data()
    if df.empty:
        st.warning("No data found in the database.")
        return

    st.subheader("Sample of movies data: Top 10 most popular movies currently in theatress")
    st.dataframe(df.sort_values(by='popularity', ascending=False).head(10).drop(columns=['popularity']))

    # Get unique languages for later selectors
    languages = sorted(df['original_language'].unique())

    # Get genres for later selectors
    genres = load_genres()

    # Create two columns for plots
    col1, col2 = st.columns(2)

    # Constant for storing the height of each figure
    PLOT_HEIGHT = 450

    # First column - Popularity histogram
    with col1:
        # First plot
        st.subheader("Popularity vs rating comparisson")
        fig_pop_vs_rating = plot_popularity_vs_rating(df)
        fig_pop_vs_rating.update_layout(
            xaxis_title="Rating (1 to 10)",
            yaxis_title="Popularity Index",
            showlegend=False,
            height=PLOT_HEIGHT
        )
        st.plotly_chart(fig_pop_vs_rating, use_container_width=True)

        # Second plot
        st.subheader("Percentage of available genres")
        genre_df = load_genre_distribution()
        fig_genre_pie = plot_genre_pie_chart(genre_df)
        fig_genre_pie.update_layout(height=PLOT_HEIGHT)
        st.plotly_chart(fig_genre_pie, use_container_width=True)
        
        # Third plot
        st.subheader("Most popular movies by genre")
        pop_genre_selector = st.selectbox(
            "Select Genre:",
            options=genres,
            index=0
        )
        genres_popularity_df = load_popularity_by_genre(pop_genre_selector)
        st.dataframe(genres_popularity_df)

    # Second column - Top languages
    with col2:
        # First plot
        st.subheader("Top 10 languages by number of movies")
        fig_lang_bar = plot_top_languages_bar(df, top_n=10)
        fig_lang_bar.update_layout(height=PLOT_HEIGHT)
        st.plotly_chart(fig_lang_bar, use_container_width=True)

        # Second plot
        st.subheader("Rating distribution by top 5 most common languages")
        fig_lang_rating_dist = plot_top_lang_rating_dist(df)
        fig_lang_rating_dist.update_layout(height=PLOT_HEIGHT)
        st.plotly_chart(fig_lang_rating_dist, use_container_width=True)

        # Third plot
        st.subheader("Most popular movies by language")
        pop_lang_selector = st.selectbox(
            "Select Language:",
            options=languages,
            index=14
        )        
        filtered_df = df[df['original_language'] == pop_lang_selector]
        st.dataframe(filtered_df.sort_values(by='popularity', ascending=False).head(10).drop(columns=['original_language', 'popularity']))

if __name__ == "__main__":
    main()
