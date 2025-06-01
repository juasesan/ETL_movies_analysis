import sqlite3

conn = sqlite3.connect("/home/juasesan/python_projects/ETL_movies_analysis/data/movies.db")
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
print(cursor.fetchall())
