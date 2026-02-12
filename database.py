import os
import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL")

conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = True
cursor = conn.cursor()


def create_tables():
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        user_id BIGINT PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        mafia_nick TEXT
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS games (
        game_id SERIAL PRIMARY KEY,
        game_name TEXT,
        game_date TEXT
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS registrations (
        user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
        game_id INTEGER REFERENCES games(game_id) ON DELETE CASCADE,
        PRIMARY KEY (user_id, game_id)
    );
    """)
