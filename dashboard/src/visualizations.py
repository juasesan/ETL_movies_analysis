import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px

def plot_popularity_distribution(df):
    fig = px.histogram(df, x="popularity", nbins=20)
    return fig

def plot_vote_vs_popularity(df):
    fig = px.scatter(df, x='popularity', y='vote_average',
                     hover_name='title',
                     title='Vote Average vs Popularity')
    return fig

def plot_genre_pie_chart(genre_df):
    fig = px.pie(genre_df, names='genre_name', values='count')
    return fig
