import os
import sqlite3
from pathlib import Path
from etl.functions import etl_process
from etl.database import MovieDatabase

def create_database():
    """Create the SQLite database and its tables."""
    # Get the project root directory
    project_root = Path(__file__).parent.parent
    db_path = project_root / "data" / "movies.db"
    
    # Ensure the data directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Create database connection
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create movies table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS movies(
        id INTEGER NOT NULL PRIMARY KEY,
        title TEXT NOT NULL,
        release_date DATE,
        original_language CHAR(2),
        adult INTEGER CHECK (adult IN (0, 1)),
        video INTEGER CHECK (video IN (0, 1)),
        popularity FLOAT,
        vote_average FLOAT,
        vote_count INTEGER
    );
    """)
    
    # Create genres table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS genres(
        genre_id INTEGER PRIMARY KEY,
        genre_name TEXT
    );
    """)
    
    # Create movies_genres table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS movies_genres(
        movie_id INTEGER,
        genre_id INTEGER,
        PRIMARY KEY (movie_id, genre_id),
        FOREIGN KEY (movie_id) REFERENCES Movies(movie_id),
        FOREIGN KEY (genre_id) REFERENCES Genres(genre_id)
    );
    """)
    
    conn.commit()
    conn.close()
    
    print("Database and tables created successfully.")

def main():
    """Main function to run the ETL process."""
    try:
        # Create database and tables
        create_database()
        
        # Run ETL process
        # You can adjust the number of pages as needed
        etl_process(num_pages=10)
        
        print("ETL process completed successfully!")
        
    except Exception as e:
        print(f"An error occurred during the ETL process: {str(e)}")

if __name__ == "__main__":
    main() 