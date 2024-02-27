import requests
import pandas as pd
import matplotlib.pyplot as plt


def extract_data_from_API(num_pages):
    headers = {
        "accept": "application/json",
    }

    with open("key.txt") as file: 
        headers['Authorization'] = file.readline()

    # Retrieving movies list
    movies = []
    for i in range(num_pages):
        url = f"https://api.themoviedb.org/3/movie/now_playing?language=en-US&page={i+1}"

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            response = response.json()
            movies += response['results']   

        else:
            print(f"Requesting movies failed in iteration {i+1} with status code: {response.status_code}")
    
    # Retrieving genres list
    url_genres = "https://api.themoviedb.org/3/genre/movie/list?language=en"

    genres = requests.get(url_genres, headers=headers)

    if genres.status_code == 200:
        genres = genres.json()
        genres = pd.DataFrame(genres['genres'])

    else:
        print(f"Requesting genres failed with status code: {genres.status_code}")
    
    return pd.DataFrame(movies), pd.DataFrame(genres)



def transform_data(data_df):
    # Removing unnecessary columns
    data_df.drop(columns=['backdrop_path', 'overview', 'original_title', 'poster_path'], inplace=True)
    
    # Transforming dates to datetime
    data_df['release_date'] = pd.to_datetime(data_df['release_date'])

    return data_df