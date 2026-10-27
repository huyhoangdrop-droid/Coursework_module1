"""Load generated streaming JSONL files into PostgreSQL for CDC."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from psycopg2.extras import Json, execute_values

from postgres_client import PostgreSQLClient


EXCLUDED_FIELDS = {
    "label", "labels", "ground_truth", "arrival_time", "is_late",
    "is_mobile", "delivery_delay_seconds", "is_burst", "batch_id",
    "is_drift_period",
}


def clean_event(event: dict[str, Any], source: Path, line_number: int) -> dict[str, Any]:
    event = {key: value for key, value in event.items() if key not in EXCLUDED_FIELDS}
    if event.get("event_id") is None:
        raise ValueError(f"{source}:{line_number}: missing event_id")
    if not event.get("event_time"):
        raise ValueError(f"{source}:{line_number}: missing event_time")
    return event


def read_rows(path: Path) -> list[tuple[int, str, Json]]:
    rows_by_event_id: dict[int, tuple[int, str, Json]] = {}
    with path.open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                event = clean_event(json.loads(line), path, line_number)
            except json.JSONDecodeError as error:
                raise ValueError(f"{path}:{line_number}: invalid JSON: {error}") from error
            event_id = int(event["event_id"])
            rows_by_event_id[event_id] = (event_id, event["event_time"], Json(event))
    return list(rows_by_event_id.values())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input", type=Path, default=Path("batch_processing/stream_input"),
        help="Folder containing generated events_*.json JSONL files.",
    )
    parser.add_argument("--batch-size", type=int, default=1000)
    args = parser.parse_args()
    if args.batch_size < 1:
        parser.error("--batch-size must be greater than zero")

    load_dotenv()
    client = PostgreSQLClient(
        database=os.getenv("POSTGRES_DB", "testdb"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
    )
    input_dir = Path(r"C:\Users\HUY HOANG\Downloads\mini_coursework\batch_processing\stream_input")
    files = sorted(input_dir.glob("events_*.json"))
    if not files:
        raise FileNotFoundError(f"No events_*.json files found in {args.input.resolve()}")
    total = 0
    with client.create_conn() as conn:
        with conn.cursor() as cursor:
            # Ensure the table exists even if the setup script has not been run yet.
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS public.stream_events (
                    event_id BIGINT PRIMARY KEY,
                    event_time TIMESTAMPTZ NOT NULL,
                    event_data JSONB NOT NULL
                )
            """)
            for path in files:
                rows = read_rows(path)
                for offset in range(0, len(rows), args.batch_size):
                    batch = rows[offset:offset + args.batch_size]
                    execute_values(cursor, """
                        INSERT INTO public.stream_events (event_id, event_time, event_data)
                        VALUES %s
                        ON CONFLICT (event_id) DO UPDATE SET
                            event_time = EXCLUDED.event_time,
                            event_data = EXCLUDED.event_data
                    """, batch)
                conn.commit()
                total += len(rows)
                print(f"Upserted {len(rows):,} events from {path.name}")

    print(f"Finished: upserted {total:,} event rows from {len(files)} file(s)")


if __name__ == "__main__":
    main()
