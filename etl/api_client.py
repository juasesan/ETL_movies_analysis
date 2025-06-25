# src/etl/api_client.py
import os
import requests
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv

class TMDBApiClient:
    """Client for interacting with The Movie Database (TMDB) API."""
    
    BASE_URL = "https://api.themoviedb.org/3"
    
    def __init__(self):
        """Initialize the API client with authentication from environment variables."""
        load_dotenv()
        self.api_key = os.getenv("TMDB_API_KEY")
        if not self.api_key:
            raise ValueError("TMDB_API_KEY environment variable is not set")
    
    @property
    def headers(self) -> Dict[str, str]:
        """Get the authorization headers for API requests."""
        return {
            "accept": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }
    
    def get_now_playing_movies(self) -> List[Dict[str, Any]]:
        """
        Get movies that are now playing in theaters.
            
        Returns:
            List of movie dictionaries
        """
        endpoint = f"{self.BASE_URL}/movie/now_playing"
        movies = []

        # Make a first GET to obtain the number of pages
        try:
            response = requests.get(f"{endpoint}?language=en-US&page=1", headers=self.headers)
            response.raise_for_status()
            total_pages = response.json()['total_pages']
            movies.extend(response.json()['results'])
        except requests.exceptions.RequestException as e:
            print(f"Error fetching the total number of pages: {str(e)}")
        
        for page in range(2, total_pages + 1):
            url = f"{endpoint}?language=en-US&page={page}"
            try:
                response = requests.get(url, headers=self.headers)
                response.raise_for_status()
                movies.extend(response.json()['results'])
            except requests.exceptions.RequestException as e:
                print(f"Error fetching movies from page {page}: {str(e)}")
                continue
        
        if not movies:
            raise ValueError("No movies data was successfully retrieved")
            
        return movies
    
    def get_movie_genres(self) -> List[Dict[str, Any]]:
        """
        Get list of movie genres.
        
        Returns:
            List of genre dictionaries
        """
        endpoint = f"{self.BASE_URL}/genre/movie/list"
        
        try:
            response = requests.get(f"{endpoint}?language=en", headers=self.headers)
            response.raise_for_status()
            return response.json()['genres']
        except requests.exceptions.RequestException as e:
            print(f"Error fetching genres: {str(e)}")
            raise