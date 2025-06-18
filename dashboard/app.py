import streamlit as st
import plotly.express as px
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from src.data_loaders import *
from src.visualizations import *

# Set page config
st.set_page_config(page_title="Movies Dashboard", layout="wide")

# Sidebar for navigation (for future expansion)
#st.sidebar.title("Navigation")
#st.sidebar.markdown("- Overview\n- Add more plots later")

st.title("Now Playing movies Analysis")
st.markdown(
"""
* This dashboard vizualizes Now Playing movies data obtained from The Movie DataBase (TMDB) ⚡
* TMDB provides an API for getting a list of movies that are currently in theatres.
* To explore the back-end services that power this Dashboard, check out [this repository](https://github.com/juasesan/ETL_movies_analysis) ✨
""")

# Main dashboard
def main():
    df = load_core_movies_data()
    if df.empty:
        st.warning("No data found in the database.")
        return

    st.subheader("Sample of Movies Data")
    st.dataframe(df.head(10))

    # Create two columns for plots
    col1, col2 = st.columns(2)

    # First column - Popularity histogram
    with col1:
        st.subheader("📊 Popularity Distribution")
        st.plotly_chart(plot_popularity_distribution(df))

        st.subheader("🎭 Movie Genres")
        genre_df = load_genre_distribution()
        st.plotly_chart(plot_genre_pie_chart(genre_df), use_container_width=True)

    # Second column - Top languages
    with col2:
        st.subheader("Top 10 Original Languages")
        top_languages = df['original_language'].value_counts().head(10)
        
        # Create bar plot using plotly
        fig = px.bar(
            x=top_languages.index,
            y=top_languages.values,
            labels={'x': 'Language', 'y': 'Count'},
            color=top_languages.values,
            color_continuous_scale='Viridis'
        )
        
        # Update layout for better visualization
        fig.update_layout(
            xaxis_title="Language",
            yaxis_title="Number of Movies",
            showlegend=False
        )
        
        st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    main()
