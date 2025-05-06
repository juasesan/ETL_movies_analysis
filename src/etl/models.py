# src/etl/models.py
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Dict, Any
import pandas as pd

@dataclass
class Genre:
    """Model representing a movie genre."""
    genre_id: int
    genre_name: str
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Genre':
        """Create a Genre instance from a dictionary."""
        return cls(
            genre_id=data['id'],
            genre_name=data['name']
        )
    
    @classmethod
    def from_dataframe(cls, df: pd.DataFrame) -> List['Genre']:
        """Create a list of Genre instances from a DataFrame."""
        return [cls(genre_id=row['genre_id'], genre_name=row['genre_name']) 
                for _, row in df.iterrows()]

@dataclass
class Movie:
    """Model representing a movie."""
    id: int
    title: str
    original_language: str
    popularity: float
    release_date: datetime
    adult: bool
    video: bool
    vote_average: float
    vote_count: int
    genre_ids: Optional[List[int]] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Movie':
        """Create a Movie instance from a dictionary."""
        return cls(
            id=data['id'],
            title=data['title'],
            original_language=data['original_language'],
            popularity=data['popularity'],
            release_date=datetime.strptime(data['release_date'], '%Y-%m-%d') if data.get('release_date') else None,
            adult=data['adult'],
            video=data['video'],
            vote_average=data['vote_average'],
            vote_count=data['vote_count'],
            genre_ids=data.get('genre_ids')
        )
    
    @classmethod
    def from_dataframe(cls, df: pd.DataFrame) -> List['Movie']:
        """Create a list of Movie instances from a DataFrame."""
        result = []
        for _, row in df.iterrows():
            movie = cls(
                id=row['id'],
                title=row['title'],
                original_language=row['original_language'],
                popularity=row['popularity'],
                release_date=row['release_date'],
                adult=bool(row['adult']),
                video=bool(row['video']),
                vote_average=row['vote_average'],
                vote_count=row['vote_count']
            )
            result.append(movie)
        return result
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert Movie instance to a dictionary for database storage."""
        return {
            'id': self.id,
            'title': self.title, 
            'original_language': self.original_language,
            'popularity': self.popularity,
            'release_date': self.release_date,
            'adult': int(self.adult),
            'video': int(self.video),
            'vote_average': self.vote_average,
            'vote_count': self.vote_count
        }

@dataclass
class MovieGenre:
    """Model representing the many-to-many relationship between movies and genres."""
    movie_id: int
    genre_id: int
    
    @classmethod
    def from_movie(cls, movie: Movie) -> List['MovieGenre']:
        """Create MovieGenre instances from a Movie with genre_ids."""
        if not movie.genre_ids:
            return []
        return [cls(movie_id=movie.id, genre_id=genre_id) 
                for genre_id in movie.genre_ids]