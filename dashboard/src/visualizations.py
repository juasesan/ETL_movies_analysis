import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px
import pandas as pd


def plot_genre_pie_chart(genre_df):
    # Calculate total count and percentage for each genre
    total_count = genre_df['count'].sum()
    genre_df = genre_df.copy()
    genre_df['percentage'] = (genre_df['count'] / total_count) * 100
    
    # Separate genres with >= 5% and < 5%
    major_genres = genre_df[genre_df['percentage'] >= 5]
    minor_genres = genre_df[genre_df['percentage'] < 5]
    
    # Create "Others" category for minor genres
    if not minor_genres.empty:
        others_count = minor_genres['count'].sum()
        others_row = pd.DataFrame({
            'genre_name': ['Others (<5% each)'],
            'count': [others_count],
            'percentage': [(others_count / total_count) * 100]
        })
        
        # Combine major genres with "Others"
        filtered_df = pd.concat([major_genres, others_row], ignore_index=True)
    else:
        filtered_df = major_genres
    
    # Create pie chart
    fig = px.pie(filtered_df, names='genre_name', values='count', hole=.5)
    return fig

def plot_top_lang_rating_dist(df):
    top_5_languages = df['original_language'].value_counts().head(5).index
    top_5_lang_df = df.loc[df['original_language'].isin(top_5_languages)]
    top_5_lang_df = top_5_lang_df[top_5_lang_df['vote_average'] > 0]

    fig = px.box(top_5_lang_df, 
                x='original_language', 
                y='vote_average',
                title='Vote Average Distribution for Top 5 Most Common Languages',
                labels={'original_language': 'Original Language', 'vote_average': 'Rating (from 1 to 10)'})
    return fig

def plot_popularity_vs_rating(df):
    fig = px.scatter(
        df.head(100), 
        x="vote_average", 
        y="popularity",
	    size="vote_count", 
        color="original_language",
        hover_name="title",
        labels={'x': 'Rating (1 to 10)', 'y': 'Popularity index'},
        log_x=True, 
        size_max=60)
    return fig

def plot_top_languages_bar(df, top_n=10):
    top_languages = df['original_language'].value_counts().head(top_n)
    fig = px.bar(
        x=top_languages.index,
        y=top_languages.values,
        labels={'x': 'Language', 'y': 'Number of Movies'},
    )
    fig.update_layout(
        xaxis_title="Language",
        yaxis_title="Number of Movies",
        showlegend=False,
        height=450
    )
    return fig