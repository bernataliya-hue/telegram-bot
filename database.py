import os
import time
import psycopg2
from psycopg2.extras import Json
import datetime
import logging
from game_dates import parse_date, stored_game_date

DATABASE_URL = os.environ.get("DATABASE_URL")

def get_connection():
    if not DATABASE_URL:
        raise ValueError("❌ Не задан DATABASE_URL в переменных окружения")
    last_error = None
    for attempt in range(5):
        try:
            return psycopg2.connect(DATABASE_URL)
        except psycopg2.OperationalError as exc:
            last_error = exc
            if attempt == 4:
                raise
            time.sleep(2)
    raise last_error

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        user_id BIGINT PRIMARY KEY,
        platform TEXT DEFAULT 'telegram',
        platform_user_id BIGINT,
        first_name TEXT,
        last_name TEXT,
        mafia_nick TEXT,
        age INTEGER,
        telegram_username TEXT,
        vk_username TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS platform TEXT DEFAULT 'telegram'")
    cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS platform_user_id BIGINT")
    cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS vk_username TEXT")
    # Existing profiles receive the migration timestamp once; new profiles use the default.
    cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP")
    cursor.execute("""
        CREATE SEQUENCE IF NOT EXISTS manual_user_ids AS BIGINT
        INCREMENT BY -1 MINVALUE -9223372036854775808
        MAXVALUE -4611686018427387904 START WITH -4611686018427387904
        NO CYCLE
    """)
    cursor.execute("UPDATE users SET platform = 'telegram' WHERE platform IS NULL")
    cursor.execute("UPDATE users SET platform_user_id = user_id WHERE platform_user_id IS NULL AND platform = 'telegram'")
    cursor.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS users_platform_platform_user_id_idx
    ON users (platform, platform_user_id)
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS games (
        game_id SERIAL PRIMARY KEY,
        game_name TEXT,
        game_date TEXT,
        gathering_time TEXT,
        start_time TEXT,
        is_deleted BOOLEAN DEFAULT FALSE,
        UNIQUE(game_name, game_date)
    )
    """)
    cursor.execute("ALTER TABLE games ADD COLUMN IF NOT EXISTS gathering_time TEXT")
    cursor.execute("ALTER TABLE games ADD COLUMN IF NOT EXISTS start_time TEXT")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS registrations (
        user_id BIGINT,
        game_id INTEGER,
        status TEXT DEFAULT 'registered',
        registered_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        PRIMARY KEY(user_id, game_id)
    )
    """)

    cursor.execute("ALTER TABLE registrations ADD COLUMN IF NOT EXISTS is_late BOOLEAN DEFAULT FALSE")
    cursor.execute("ALTER TABLE registrations ADD COLUMN IF NOT EXISTS registered_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS game_hosts (
        game_id INTEGER PRIMARY KEY,
        user_id BIGINT NOT NULL
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
    CREATE TABLE IF NOT EXISTS late_players (
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
    cursor.execute("INSERT INTO settings (key, value) VALUES ('player_of_month', '') ON CONFLICT (key) DO NOTHING")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS archived_players (
            archive_id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            player_data JSONB NOT NULL,
            registrations JSONB NOT NULL,
            thinking_players JSONB NOT NULL,
            late_players JSONB NOT NULL,
            game_hosts JSONB NOT NULL,
            player_titles JSONB NOT NULL DEFAULT '{}'::jsonb,
            archived_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
        )
    """)
    cursor.execute("ALTER TABLE archived_players ADD COLUMN IF NOT EXISTS player_titles JSONB NOT NULL DEFAULT '{}'::jsonb")
    # Legacy rows contain no recoverable year. Freeze their previous interpretation
    # at upgrade time, so they cannot become upcoming again next January.
    cursor.execute(
        "INSERT INTO settings (key, value) VALUES ('legacy_game_year', %s) ON CONFLICT (key) DO NOTHING",
        (str(datetime.date.today().year),),
    )
    cursor.execute("SELECT value FROM settings WHERE key = 'legacy_game_year'")
    legacy_year = int(cursor.fetchone()[0])
    cursor.execute("SELECT game_id, game_name, game_date FROM games")
    for game_id, game_name, game_date in cursor.fetchall():
        value = (game_date or '').split()[-1:]
        if not value or len(value[0].split('.')) != 2:
            continue
        parsed = parse_date(game_date, default_year=legacy_year)
        if not parsed:
            logging.warning('Cannot migrate date for game %s: %r', game_id, game_date)
            continue
        full_date = stored_game_date(parsed)
        cursor.execute(
            "SELECT 1 FROM games WHERE game_name = %s AND game_date = %s AND game_id <> %s",
            (game_name, full_date, game_id),
        )
        if cursor.fetchone():
            logging.warning('Duplicate date during migration for game %s; keeping legacy date', game_id)
            continue
        cursor.execute("UPDATE games SET game_date = %s WHERE game_id = %s", (full_date, game_id))
    
    conn.commit()
    cursor.close()
    conn.close()
    return legacy_year


def delete_inactive_players():
    """Archive and delete players inactive for more than three months."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT u.user_id, to_jsonb(u)
            FROM users u
            LEFT JOIN (
                SELECT user_id, MAX(registered_at) AS last_registered_at
                FROM registrations GROUP BY user_id
            ) r USING (user_id)
            WHERE COALESCE(r.last_registered_at, u.created_at)
                  < clock_timestamp() - INTERVAL '3 months'
            FOR UPDATE OF u
        """)
        stale = cursor.fetchall()
        archived = []
        for user_id, player_data in stale:
            relations = {}
            for table in ("registrations", "thinking_players", "late_players", "game_hosts"):
                cursor.execute(
                    f"SELECT COALESCE(jsonb_agg(to_jsonb(t)), '[]'::jsonb) FROM {table} t WHERE user_id = %s",
                    (user_id,),
                )
                relations[table] = cursor.fetchone()[0]
            cursor.execute("""
                INSERT INTO archived_players
                    (user_id, player_data, registrations, thinking_players, late_players, game_hosts, player_titles)
                VALUES (%s, %s, %s, %s, %s, %s,
                    COALESCE((SELECT jsonb_object_agg(key, value) FROM settings
                              WHERE key IN ('player_of_month', 'player_title_month', 'player_title_mvp',
                                            'player_title_sheriff', 'player_title_don', 'player_title_red',
                                            'player_title_black', 'player_title_table_savior') AND value = %s), '{}'::jsonb))
                RETURNING archive_id
            """, (user_id, Json(player_data), *(Json(relations[t]) for t in ("registrations", "thinking_players", "late_players", "game_hosts")), str(user_id)))
            archive_id = cursor.fetchone()[0]
            archived.append((archive_id, user_id, player_data.get("first_name"), player_data.get("last_name"), player_data.get("mafia_nick")))
            for table in ("registrations", "thinking_players", "late_players", "game_hosts"):
                cursor.execute(f"DELETE FROM {table} WHERE user_id = %s", (user_id,))
            cursor.execute("DELETE FROM users WHERE user_id = %s", (user_id,))
            cursor.execute("DELETE FROM settings WHERE key IN ('player_of_month', 'player_title_month', 'player_title_mvp', 'player_title_sheriff', 'player_title_don', 'player_title_red', 'player_title_black', 'player_title_table_savior') AND value = %s", (str(user_id),))
        conn.commit()
        return archived
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def get_archived_players():
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT archive_id, user_id, player_data->>'first_name', player_data->>'last_name', player_data->>'mafia_nick' FROM archived_players ORDER BY archived_at DESC, archive_id DESC")
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


def restore_archived_player(archive_id):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT user_id, player_data, registrations, thinking_players, late_players, game_hosts, player_titles FROM archived_players WHERE archive_id = %s FOR UPDATE", (archive_id,))
        archived = cursor.fetchone()
        if not archived:
            conn.commit()
            return None
        user_id, player_data, *row_data = archived
        relations, player_titles = row_data[:-1], row_data[-1]
        cursor.execute("INSERT INTO users SELECT (jsonb_populate_record(NULL::users, %s)).* ON CONFLICT (user_id) DO NOTHING", (Json(player_data),))
        for table, rows in zip(("registrations", "thinking_players", "late_players", "game_hosts"), relations):
            cursor.execute(f"INSERT INTO {table} SELECT * FROM jsonb_populate_recordset(NULL::{table}, %s) ON CONFLICT DO NOTHING", (Json(rows),))
        if player_titles:
            cursor.execute("""
                INSERT INTO settings (key, value)
                SELECT key, value FROM jsonb_each_text(%s)
                ON CONFLICT (key) DO NOTHING
            """, (Json(player_titles),))
        cursor.execute("DELETE FROM archived_players WHERE archive_id = %s", (archive_id,))
        conn.commit()
        return user_id, player_data.get("first_name"), player_data.get("last_name"), player_data.get("mafia_nick")
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()
