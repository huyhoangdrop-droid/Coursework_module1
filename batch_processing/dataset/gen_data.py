
"""Generate a large, self-contained Instacart-like batch dataset.

Every run removes and recreates ``batch_processing/generated_dataset``.
CSV parts keep memory usage dependent on ``--chunk-size``, not total size.
"""

from __future__ import annotations

import argparse
import io
import json
import shutil
import time
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd


BATCH_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BATCH_DIR / "generated_dataset"

DEFAULT_ROWS = 100_000
DEFAULT_CHUNK_SIZE = 500_000
DEFAULT_SEED = 42
DUPLICATE_RATE = 0.02
USER_CARDINALITY_RATE = 0.80

# Twenty months makes the schema boundary exact: the first 12 months are 60%.
MONTHS = pd.date_range("2024-01-01", periods=20, freq="MS")
OLD_MONTH_COUNT = 12

CITIES = np.array(["HCMC", "Hanoi", "Da-Nang", "Can-Tho", "Hue", "Nha-Trang"])
CITY_WEIGHTS = np.array([0.85, 0.06, 0.04, 0.02, 0.02, 0.01])

# IDs/names mirror departments.csv in Instacart Market Basket Analysis.
# Product names are synthetic so the original CSV files are not required.
DEPARTMENTS = {
    1: "frozen",
    2: "other",
    3: "bakery",
    4: "produce",
    5: "alcohol",
    6: "international",
    7: "beverages",
    8: "pets",
    9: "dry goods pasta",
    10: "bulk",
    11: "personal care",
    12: "meat seafood",
    13: "pantry",
    14: "breakfast",
    15: "canned goods",
    16: "dairy eggs",
    17: "household",
    18: "babies",
    19: "snacks",
    20: "deli",
    21: "missing",
}
PRODUCE_DEPARTMENT_ID = 4
PRODUCTS_PER_DEPARTMENT = 2_400
OTHER_DEPARTMENT_IDS = np.array(
    [department_id for department_id in DEPARTMENTS if department_id != PRODUCE_DEPARTMENT_ID]
)

SHIPPING_METHODS = np.array(["standard", "express", "same_day", "pickup"])
COUPON_CODES = np.array(["NONE", "SALE10", "PROMO20", "NEWUSER"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate chunked Instacart-like CSV data without source files."
    )
    size = parser.add_mutually_exclusive_group()
    size.add_argument(
        "--rows", type=int, help="Original rows before adding duplicates."
    )
    size.add_argument(
        "--target-size-gb",
        type=float,
        help="Approximate total CSV size in decimal GB (for example: 100).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help=f"Original rows per CSV part (default: {DEFAULT_CHUNK_SIZE:,}).",
    )
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    return parser.parse_args()


def duplicate_count(start: int, size: int, total_rows: int) -> int:
    """Allocate duplicates across chunks while keeping the final rate near 2%."""
    total_duplicates = round(total_rows * DUPLICATE_RATE / (1 - DUPLICATE_RATE))
    return (
        (start + size) * total_duplicates // total_rows
        - start * total_duplicates // total_rows
    )


def generate_chunk(
    start: int,
    size: int,
    total_rows: int,
    user_count: int,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """Generate one independent chunk with deterministic global IDs."""
    order_id = np.arange(start + 1, start + size + 1, dtype=np.int64)
    user_id = (order_id - 1) % user_count + 1

    is_produce = rng.random(size) < 0.80
    department_id = rng.choice(OTHER_DEPARTMENT_IDS, size=size)
    department_id[is_produce] = PRODUCE_DEPARTMENT_ID
    local_product_id = rng.integers(1, PRODUCTS_PER_DEPARTMENT + 1, size=size)
    product_id = (department_id - 1) * PRODUCTS_PER_DEPARTMENT + local_product_id
    category = np.array([DEPARTMENTS[value] for value in department_id], dtype=object)
    product_name = np.char.add(
        np.char.add(np.char.replace(category.astype(str), " ", "_"), "_product_"),
        np.char.zfill(local_product_id.astype(str), 4),
    )

    month_index = ((order_id - 1) * len(MONTHS) // total_rows).astype(np.int16)
    old_schema = month_index < OLD_MONTH_COUNT
    coupon_code = rng.choice(COUPON_CODES, size=size).astype(object)
    shipping_method = rng.choice(SHIPPING_METHODS, size=size).astype(object)
    coupon_code[old_schema] = None
    shipping_method[old_schema] = None

    frame = pd.DataFrame(
        {
            "order_id": order_id,
            "user_id": user_id,
            "order_number": (order_id - 1) // user_count + 1,
            "order_dow": rng.integers(0, 7, size=size, dtype=np.int8),
            "order_hour_of_day": rng.integers(0, 24, size=size, dtype=np.int8),
            "days_since_prior_order": np.round(rng.uniform(0, 30, size=size), 1),
            "product_id": product_id,
            "product_name": product_name,
            "aisle_id": (product_id * 37 % 134 + 1).astype(np.int16),
            "department_id": department_id.astype(np.int8),
            "category": category,
            "add_to_cart_order": rng.integers(1, 31, size=size, dtype=np.int8),
            "reordered": rng.integers(0, 2, size=size, dtype=np.int8),
            "city": rng.choice(CITIES, size=size, p=CITY_WEIGHTS),
            "amount": np.round(rng.uniform(5, 500, size=size), 2),
            "order_month": MONTHS[month_index].strftime("%Y-%m"),
            "schema_version": np.where(old_schema, 1, 2).astype(np.int8),
            "coupon_code": coupon_code,
            "shipping_method": shipping_method,
        }
    )

    copies = duplicate_count(start, size, total_rows)
    if copies:
        duplicate_positions = rng.integers(0, size, size=copies)
        frame = pd.concat([frame, frame.iloc[duplicate_positions]], ignore_index=True)
    return frame


def estimate_bytes_per_output_row(seed: int) -> float:
    """Serialize a representative sample to estimate rows for a size target."""
    sample_rows = 20_000
    sample = generate_chunk(
        start=0,
        size=sample_rows,
        total_rows=sample_rows,
        user_count=int(sample_rows * USER_CARDINALITY_RATE),
        rng=np.random.default_rng(seed),
    )
    # Old parts genuinely use the old schema: the two new columns do not exist.
    # Serialize both schema versions so --target-size-gb reflects actual output.
    old_sample = sample[sample["schema_version"] == 1].drop(
        columns=["coupon_code", "shipping_method"]
    )
    new_sample = sample[sample["schema_version"] == 2]
    total_bytes = 0
    for part in (old_sample, new_sample):
        buffer = io.BytesIO()
        part.to_csv(buffer, index=False, lineterminator="\n")
        total_bytes += buffer.tell()
    return total_bytes / len(sample)


def resolve_source_rows(args: argparse.Namespace, bytes_per_row: float) -> int:
    if args.rows is not None:
        if args.rows <= 0:
            raise ValueError("--rows must be greater than 0.")
        return args.rows
    if args.target_size_gb is not None:
        if args.target_size_gb <= 0:
            raise ValueError("--target-size-gb must be greater than 0.")
        target_bytes = args.target_size_gb * 1_000_000_000
        # The calibration sample has 5-digit IDs. Account for wider order_id and
        # user_id values at billion-row scale, then converge on a row estimate.
        source_rows = max(
            1, round(target_bytes / bytes_per_row * (1 - DUPLICATE_RATE))
        )
        for _ in range(3):
            extra_id_bytes = 2 * max(0, len(str(source_rows)) - 5)
            adjusted_bytes_per_row = bytes_per_row + extra_id_bytes
            source_rows = max(
                1,
                round(
                    target_bytes
                    / adjusted_bytes_per_row
                    * (1 - DUPLICATE_RATE)
                ),
            )
        return source_rows
    return DEFAULT_ROWS


def recreate_output_dir() -> None:
    """Safely remove only the fixed generated_dataset directory."""
    expected = (BATCH_DIR / "generated_dataset").resolve()
    actual = OUTPUT_DIR.resolve()
    if actual != expected or actual.name != "generated_dataset":
        raise RuntimeError(f"Refusing to remove unexpected output path: {actual}")
    if actual.exists():
        shutil.rmtree(actual)
    actual.mkdir(parents=True)


def ensure_disk_space(estimated_bytes: int) -> None:
    free_bytes = shutil.disk_usage(BATCH_DIR).free
    required_bytes = int(estimated_bytes * 1.10)
    if free_bytes < required_bytes:
        raise OSError(
            "Not enough free disk space: "
            f"need about {required_bytes / 1_000_000_000:.2f} GB including buffer, "
            f"but only {free_bytes / 1_000_000_000:.2f} GB is free."
        )


def main() -> None:
    args = parse_args()
    if args.chunk_size <= 0:
        raise ValueError("--chunk-size must be greater than 0.")

    bytes_per_row = estimate_bytes_per_output_row(args.seed)
    source_rows = resolve_source_rows(args, bytes_per_row)
    user_count = max(1, int(source_rows * USER_CARDINALITY_RATE))
    expected_output_rows = source_rows + round(
        source_rows * DUPLICATE_RATE / (1 - DUPLICATE_RATE)
    )
    estimated_bytes = round(expected_output_rows * bytes_per_row)
    ensure_disk_space(estimated_bytes)
    recreate_output_dir()

    print(f"Original rows       : {source_rows:,}")
    print(f"Expected output rows: {expected_output_rows:,}")
    print(f"Estimated CSV size  : {estimated_bytes / 1_000_000_000:.2f} GB")
    print(f"Output directory    : {OUTPUT_DIR}")

    rng = np.random.default_rng(args.seed)
    city_counts: Counter[str] = Counter()
    category_counts: Counter[str] = Counter()
    schema_counts: Counter[str] = Counter()
    output_files: list[str] = []
    total_output_rows = 0
    total_csv_bytes = 0
    started = time.perf_counter()

    # Force a part boundary at the schema transition. This ensures old CSV files
    # omit the new columns completely and new CSV files contain them.
    schema_transition = (source_rows * OLD_MONTH_COUNT + len(MONTHS) - 1) // len(MONTHS)
    start = 0
    chunk_id = 1
    while start < source_rows:
        size = min(args.chunk_size, source_rows - start)
        if start < schema_transition < start + size:
            size = schema_transition - start
        frame = generate_chunk(start, size, source_rows, user_count, rng)
        output_path = OUTPUT_DIR / f"instacart_part_{chunk_id:06d}.csv"
        frame_to_write = frame
        if frame["schema_version"].iat[0] == 1:
            frame_to_write = frame.drop(columns=["coupon_code", "shipping_method"])
        frame_to_write.to_csv(output_path, index=False, lineterminator="\n")

        output_files.append(output_path.name)
        total_output_rows += len(frame)
        total_csv_bytes += output_path.stat().st_size
        city_counts.update(frame["city"])
        category_counts.update(frame["category"])
        schema_counts.update(str(value) for value in frame["schema_version"])
        elapsed = max(time.perf_counter() - started, 0.001)
        print(
            f"[{chunk_id:06d}] {total_output_rows:,}/{expected_output_rows:,} rows | "
            f"{total_csv_bytes / 1_000_000_000:.2f} GB | "
            f"{total_output_rows / elapsed:,.0f} rows/s"
        )
        del frame
        start += size
        chunk_id += 1

    report = {
        "generator": "synthetic Instacart Market Basket Analysis-like data",
        "seed": args.seed,
        "source_rows": source_rows,
        "duplicate_rows": total_output_rows - source_rows,
        "duplicate_rate": (total_output_rows - source_rows) / total_output_rows,
        "total_rows": total_output_rows,
        "unique_users": user_count,
        "unique_orders": source_rows,
        "csv_size_bytes": total_csv_bytes,
        "csv_size_gb": total_csv_bytes / 1_000_000_000,
        "chunk_size": args.chunk_size,
        "output_files": output_files,
        "city_counts": dict(city_counts),
        "category_counts": dict(category_counts),
        "schema_version_counts": dict(schema_counts),
    }
    report_path = OUTPUT_DIR / "generation_report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Completed in {(time.perf_counter() - started) / 60:.2f} minutes")
    print(f"Report: {report_path}")


if __name__ == "__main__":
    main()