"""Create the PostgreSQL table used by the streaming CDC pipeline."""

from __future__ import annotations

import os

from dotenv import load_dotenv

from postgres_client import PostgreSQLClient


TABLE_NAME = "public.stream_events"


def main() -> None:
    load_dotenv()
    client = PostgreSQLClient(
        database=os.getenv("POSTGRES_DB", "testdb"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
    )

    conn = client.create_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                    event_id BIGINT PRIMARY KEY,
                    event_time TIMESTAMPTZ NOT NULL,
                    event_data JSONB NOT NULL
                )
            """)
            cursor.execute(f"ALTER TABLE {TABLE_NAME} REPLICA IDENTITY FULL")
            cursor.execute("""
                DO $$ BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_publication
                        WHERE pubname = 'stream_events_publication'
                    ) THEN
                        CREATE PUBLICATION stream_events_publication
                        FOR TABLE public.stream_events;
                    END IF;
                END $$
            """)
        conn.commit()
    finally:
        conn.close()

    print(f"Created/verified {TABLE_NAME} and stream_events_publication")


if __name__ == "__main__":
    main()
