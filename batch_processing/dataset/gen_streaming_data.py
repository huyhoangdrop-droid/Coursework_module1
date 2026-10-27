"""Generate scalable Instacart-like JSON micro-batches for Spark streaming."""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd


DEPARTMENTS = {
    1: "frozen", 2: "other", 3: "bakery", 4: "produce", 5: "alcohol",
    6: "international", 7: "beverages", 8: "pets", 9: "dry goods pasta",
    10: "bulk", 11: "personal care", 12: "meat seafood", 13: "pantry",
    14: "breakfast", 15: "canned goods", 16: "dairy eggs", 17: "household",
    18: "babies", 19: "snacks", 20: "deli", 21: "missing",
}
CITIES = np.array(["HCMC", "Hanoi", "Da-Nang", "Can-Tho", "Hue", "Nha-Trang"])
CITY_WEIGHTS = np.array([0.65, 0.14, 0.08, 0.05, 0.04, 0.04])
COUPONS = np.array(["NONE", "SALE10", "PROMO20", "NEWUSER"])
SHIPPING = np.array(["standard", "express", "same_day", "pickup"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("stream_input"))
    parser.add_argument("--n-customers", type=int, default=120_000)
    parser.add_argument("--n-products", type=int, default=50_400,
                        help="Use the batch generator's 21 x 2,400 product ID space.")
    parser.add_argument("--days-history", type=int, default=180)
    parser.add_argument("--batches", type=int, default=100)
    parser.add_argument("--rows-per-batch", type=int, default=2_000)
    parser.add_argument("--interval-seconds", type=float, default=1.0)
    parser.add_argument("--duplicate-rate", type=float, default=0.015)
    parser.add_argument("--late-rate", type=float, default=0.0,
                        help="Optional extra long-tail late events for stress tests; default is off.")
    parser.add_argument("--max-late-hours", type=float, default=72)
    parser.add_argument("--mobile-delay-rate", type=float, default=0.15)
    parser.add_argument("--mobile-delay-minutes", type=float, default=5)
    parser.add_argument("--mobile-delay-max-minutes", type=float, default=10)
    parser.add_argument("--start-time", default="2025-09-01T00:00:00Z")
    parser.add_argument("--burst-probability", type=float, default=0.08)
    parser.add_argument("--burst-multiplier", type=int, default=8)
    parser.add_argument("--schema-v2-after", type=float, default=0.60)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--no-sleep", action="store_true")
    args = parser.parse_args()

    if min(args.n_customers, args.n_products, args.days_history,
           args.batches, args.rows_per_batch, args.burst_multiplier) <= 0:
        parser.error("scale and batch arguments must be greater than zero")
    if args.mobile_delay_minutes < 0 or args.mobile_delay_max_minutes < args.mobile_delay_minutes:
        parser.error("mobile delay bounds must satisfy 0 <= min <= max")
    for name in ("duplicate_rate", "late_rate", "burst_probability", "schema_v2_after", "mobile_delay_rate"):
        if not 0 <= getattr(args, name) < 1:
            parser.error(f"--{name.replace('_', '-')} must be in [0, 1)")
    return args


def make_catalog(n_products: int, rng: np.random.Generator) -> dict[str, np.ndarray]:
    # Match gen_data.py: product IDs are grouped into 2,400-item department lanes.
    product_department = np.minimum((np.arange(n_products) // 2_400) + 1, 21)

    # Zipf weights create hot products and a long tail like basket data.
    popularity = np.arange(1, n_products + 1, dtype=np.float64) ** -1.08
    popularity[product_department == 4] *= 5  # produce is the hot department
    popularity /= popularity.sum()
    return {
        "department": product_department,
        "category": np.array([DEPARTMENTS[x] for x in product_department]),
        "aisle": rng.integers(1, 135, n_products, dtype=np.int16),
        "popularity": popularity,
    }


def generate_batch(
    args: argparse.Namespace,
    rng: np.random.Generator,
    catalog: dict[str, np.ndarray],
    customer_orders: np.ndarray,
    batch_id: int,
    clock_start: datetime,
    clock_end: datetime,
    first_order_id: int,
) -> tuple[pd.DataFrame, int, int]:
    burst = rng.random() < args.burst_probability
    rows = args.rows_per_batch * (args.burst_multiplier if burst else 1)

    if not hasattr(args, "user_weights"):
        activity = rng.lognormal(0.0, 1.15, args.n_customers)
        args.user_weights = activity / activity.sum()
        args.user_favorite = rng.integers(0, args.n_products, args.n_customers)
        args.user_reorder_propensity = rng.beta(5, 3, args.n_customers)
        args.user_cycle_days = rng.integers(5, 31, args.n_customers)
    user_id = rng.choice(args.n_customers, rows, p=args.user_weights) + 1
    within_batch = pd.Series(user_id).groupby(user_id, sort=False).cumcount().to_numpy()
    order_number = customer_orders[user_id - 1] + within_batch + 1
    customer_orders += np.bincount(user_id - 1, minlength=args.n_customers)

    event_seconds = rng.uniform(clock_start.timestamp(), clock_end.timestamp(), rows)
    product_index = rng.choice(args.n_products, rows, p=catalog["popularity"])
    user_index = user_id - 1
    favorite = rng.random(rows) < 0.42
    product_index[favorite] = args.user_favorite[user_index[favorite]]
    drifted = np.full(rows, batch_id >= int(args.batches * 0.8))
    if drifted.any():
        beverages = np.flatnonzero(catalog["department"] == 7)
        product_index[drifted] = rng.choice(beverages, drifted.sum())
    department_id = catalog["department"][product_index]
    legacy_late = rng.random(rows) < args.late_rate
    event_seconds[legacy_late] -= rng.uniform(1, args.max_late_hours * 3600, legacy_late.sum())
    mobile = rng.random(rows) < args.mobile_delay_rate
    delay_seconds = np.zeros(rows)
    delay_seconds[mobile] = rng.uniform(args.mobile_delay_minutes * 60,
                                         args.mobile_delay_max_minutes * 60, mobile.sum())
    delay_seconds[legacy_late] += rng.uniform(1, args.max_late_hours * 3600,
                                               legacy_late.sum())
    arrival_seconds = event_seconds + delay_seconds
    event_time = pd.to_datetime(event_seconds, unit="s", utc=True).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )

    order_id = np.arange(first_order_id, first_order_id + rows, dtype=np.int64)
    frame = pd.DataFrame({
        "event_id": order_id,
        "order_id": order_id,
        "user_id": user_id,
        "order_number": order_number,
        "order_dow": pd.to_datetime(event_time).dayofweek.astype("int8"),
        "order_hour_of_day": pd.to_datetime(event_time).hour.astype("int8"),
        "days_since_prior_order": np.minimum(
            args.user_cycle_days[user_index] * rng.lognormal(0, 0.12, rows), 30
        ).round(1),
        "product_id": product_index + 1,
        "aisle_id": catalog["aisle"][product_index],
        "department_id": department_id,
        "category": catalog["category"][product_index],
        "add_to_cart_order": rng.integers(1, 31, rows, dtype=np.int16),
        "reordered": (rng.random(rows) < args.user_reorder_propensity[user_index]).astype(np.int8),
        "city": rng.choice(CITIES, rows, p=CITY_WEIGHTS),
        "amount": np.round(rng.uniform(5, 500, rows), 2),
        "event_time": event_time,
        "arrival_time": pd.to_datetime(arrival_seconds, unit="s", utc=True).strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        ),
        "is_late": delay_seconds > 0,
        "is_mobile": mobile,
        "delivery_delay_seconds": np.round(delay_seconds, 3),
        "is_drift_period": drifted,
        "is_burst": burst,
        "batch_id": batch_id,
    })

    schema_v2 = batch_id >= int(args.batches * args.schema_v2_after)
    frame["schema_version"] = 2 if schema_v2 else 1
    if schema_v2:
        frame["coupon_code"] = rng.choice(COUPONS, rows)
        frame["shipping_method"] = rng.choice(SHIPPING, rows)

    duplicate_rows = round(rows * args.duplicate_rate / (1 - args.duplicate_rate))
    if duplicate_rows:
        duplicates = frame.iloc[rng.integers(0, rows, duplicate_rows)].copy()
        # Same logical event and event time, independently observed later.
        duplicate_arrival = pd.to_datetime(duplicates["arrival_time"], utc=True)
        duplicates["arrival_time"] = (duplicate_arrival + pd.to_timedelta(
            rng.uniform(1, 20, duplicate_rows), unit="s"
        )).dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        frame = pd.concat([frame, duplicates], ignore_index=True)
        frame = frame.iloc[rng.permutation(len(frame))].reset_index(drop=True)
    return frame, rows, duplicate_rows


def write_atomic(frame: pd.DataFrame, output: Path, run_id: str, batch_id: int) -> Path:
    final = output / f"events_{run_id}_{batch_id:06d}.json"
    temporary = output / f".{final.name}.tmp"
    frame.to_json(temporary, orient="records", lines=True, date_format="iso")
    os.replace(temporary, final)  # Spark only sees a completely written file.
    return final


def write_labels(
    events: list[pd.DataFrame], output: Path, run_id: str, timeline_end: datetime
) -> Path:
    """Write user/date labels using unique events and the following 7 UTC days."""
    labels_dir = output / "labels"
    labels_dir.mkdir(parents=True, exist_ok=True)
    labels_path = labels_dir / f"stream_labels_{run_id}.csv"
    if not events:
        pd.DataFrame(columns=["user_id", "label_date", "label"]).to_csv(labels_path, index=False)
        return labels_path

    clean = pd.concat(events, ignore_index=True).drop_duplicates("event_id")
    clean["event_time"] = pd.to_datetime(clean["event_time"], utc=True)
    clean["event_date"] = clean["event_time"].dt.floor("D")
    output_rows: list[tuple[int, str, int]] = []
    for user_id, group in clean.groupby("user_id", sort=False):
        event_ns = np.sort(group["event_time"].astype("int64").to_numpy())
        label_dates = np.sort(group["event_date"].astype("int64").unique())
        # Do not emit labels whose full 7-day outcome horizon falls outside replay.
        end_ns = pd.Timestamp(timeline_end).value
        label_dates = label_dates[label_dates + 8 * 86_400_000_000_000 <= end_ns]
        for date_ns in label_dates:
            lower = np.searchsorted(event_ns, date_ns + 86_400_000_000_000, side="left")
            upper = np.searchsorted(event_ns, date_ns + 8 * 86_400_000_000_000, side="left")
            label_date = pd.Timestamp(date_ns, tz="UTC").strftime("%Y-%m-%d")
            output_rows.append((int(user_id), label_date, int(upper > lower)))
    pd.DataFrame(output_rows, columns=["user_id", "label_date", "label"]).to_csv(
        labels_path, index=False
    )
    return labels_path


def main() -> None:
    args = parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    metadata = args.output / "_metadata"
    metadata.mkdir(exist_ok=True)

    rng = np.random.default_rng(args.seed)
    catalog = make_catalog(args.n_products, rng)
    customer_orders = np.zeros(args.n_customers, dtype=np.int32)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    history_start = datetime.fromisoformat(args.start_time.replace("Z", "+00:00"))
    if history_start.tzinfo is None:
        raise ValueError("--start-time must include a UTC offset, such as Z.")
    history_start = history_start.astimezone(timezone.utc)
    config = {key: (str(value) if isinstance(value, Path) else value)
              for key, value in vars(args).items()}
    config.update({
        "timeline_start_utc": history_start.isoformat(),
        "timeline_end_utc": (history_start + timedelta(days=args.days_history)).isoformat(),
        "mobile_delay_distribution": (
            f"{args.mobile_delay_rate:.0%} of events: uniform "
            f"[{args.mobile_delay_minutes}, {args.mobile_delay_max_minutes}] minutes"
        ),
        "recommended_flink_allowed_lateness": "15 minutes",
        "feature_windows": ["1d", "3d", "7d"],
        "feature_keys": ["user_id", "product_id"],
        "idempotency_key": "event_id (equal to order_id)",
        "duplicate_rate": args.duplicate_rate,
        "behavior_model": "persistent lognormal user activity, per-user favorites/reorder propensity/reorder cycle, final-20%-period product-mix drift",
    })
    (metadata / f"config_{run_id}.json").write_text(
        json.dumps(config, indent=2), encoding="utf-8"
    )

    clock_step = timedelta(days=args.days_history / args.batches)
    next_order_id = 1
    total_rows = total_duplicates = 0
    clean_event_frames: list[pd.DataFrame] = []
    ground_truth_dir = args.output / "ground_truth"
    ground_truth_dir.mkdir(parents=True, exist_ok=True)
    ground_truth_columns = [
        "event_id", "arrival_time", "is_late", "is_mobile", "delivery_delay_seconds",
        "is_burst", "batch_id", "is_drift_period",
    ]

    try:
        for batch_id in range(args.batches):
            clock_start = history_start + batch_id * clock_step
            clock_end = clock_start + clock_step
            frame, new_rows, duplicates = generate_batch(
                args, rng, catalog, customer_orders, batch_id,
                clock_start, clock_end, next_order_id,
            )
            clean_event_frames.append(frame.drop_duplicates("event_id")[[
                "event_id", "user_id", "product_id", "event_time"
            ]].copy())
            ground_truth_path = ground_truth_dir / f"ground_truth_{run_id}_{batch_id:06d}.json"
            frame[ground_truth_columns].to_json(
                ground_truth_path, orient="records", lines=True, date_format="iso"
            )
            payload_columns = [column for column in frame.columns if column not in {
                "product_name", "order_month", "is_late", "is_burst", "batch_id",
                "is_mobile", "delivery_delay_seconds", "is_drift_period",
            }]
            path = write_atomic(frame[payload_columns], args.output, run_id, batch_id)
            next_order_id += new_rows
            total_rows += len(frame)
            total_duplicates += duplicates
            print(
                f"batch={batch_id:06d} rows={len(frame):,} late={int(frame['is_late'].sum()):,} "
                f"duplicates={duplicates:,} burst={bool(frame['is_burst'].iloc[0])} file={path.name}"
            )
            if not args.no_sleep and batch_id + 1 < args.batches:
                time.sleep(args.interval_seconds)
    except KeyboardInterrupt:
        print("Stopped by user")
    finally:
        label_path = write_labels(
            clean_event_frames, args.output, run_id,
            history_start + timedelta(days=args.days_history),
        )
        print(f"Generated rows={total_rows:,}, duplicates={total_duplicates:,}, labels={label_path}, output={args.output}")


if __name__ == "__main__":
    main()
