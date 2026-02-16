import os
import psycopg2
from psycopg2.extras import RealDictCursor
from urllib.parse import urlparse

DATABASE_URL = os.environ.get("DATABASE_URL")

def get_connection():
    return psycopg2.connect(DATABASE_URL)

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        user_id BIGINT PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        mafia_nick TEXT,
        age INTEGER,
        telegram_username TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS games (
        game_id SERIAL PRIMARY KEY,
        game_name TEXT,
        game_date TEXT,
        is_deleted BOOLEAN DEFAULT FALSE,
        UNIQUE(game_name, game_date)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS registrations (
        user_id BIGINT,
        game_id INTEGER,
        status TEXT DEFAULT 'registered',
        PRIMARY KEY(user_id, game_id)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS thinking_players (
        user_id BIGINT,
        game_id INTEGER,
        PRIMARY KEY(user_id, game_id)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    """)
    cursor.execute("INSERT INTO settings (key, value) VALUES ('schedule', 'Расписание пока не установлено') ON CONFLICT (key) DO NOTHING")
    
    conn.commit()
    cursor.close()
    conn.close()
