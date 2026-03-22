from typing import Optional

import psycopg2
from psycopg2.extensions import connection as _Connection

from config import DATABASE_URL


def get_connection() -> _Connection:
    return psycopg2.connect(DATABASE_URL)


def init_db() -> None:
    """Создание таблиц, если их ещё нет."""
    conn = get_connection()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS user_mapping (
                    id SERIAL PRIMARY KEY,
                    moysklad_owner_id VARCHAR(255) UNIQUE NOT NULL,
                    amocrm_user_id BIGINT NOT NULL
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS sync_state (
                    sync_key VARCHAR(255) PRIMARY KEY,
                    sync_value TEXT NOT NULL
                );
                """
            )
    finally:
        conn.close()


def get_amocrm_user_id_by_ms_owner(owner_id: str) -> Optional[int]:
    """Получить amo_user_id по идентификатору owner из МойСклад."""
    conn = get_connection()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "SELECT amocrm_user_id FROM user_mapping WHERE moysklad_owner_id = %s",
                (owner_id,),
            )
            row = cur.fetchone()
            return int(row[0]) if row else None
    finally:
        conn.close()


def get_sync_state(key: str) -> Optional[str]:
    conn = get_connection()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT sync_value FROM sync_state WHERE sync_key = %s", (key,))
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        conn.close()


def set_sync_state(key: str, value: str) -> None:
    conn = get_connection()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO sync_state (sync_key, sync_value)
                VALUES (%s, %s)
                ON CONFLICT (sync_key)
                DO UPDATE SET sync_value = EXCLUDED.sync_value
                """,
                (key, value),
            )
    finally:
        conn.close()

