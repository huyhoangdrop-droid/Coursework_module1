to gen 100gb dataset size. run this

Examples:
    python batch_processing/dataset/gen_data.py
    python batch_processing/dataset/gen_data.py --rows 10000000
    python batch_processing/dataset/gen_data.py --target-size-gb 100

## Streaming event data

`gen_streaming_data.py` creates JSON micro-batches on an event-time timeline.
The default timeline starts at `2025-09-01T00:00:00Z`, immediately after the
batch generator's final month (`2025-08`). Set `--start-time` to the exact UTC
cutover `T` used by the offline materialization if that boundary changes.
Keep the same `user_id` and `product_id` key definitions when materializing
both sources.

By default, 15% of events are mobile and arrive uniformly 5–10 minutes after
their `event_time`; all other events arrive at event time. Configure Flink
`allowedLateness` to 15 minutes. The optional `--late-rate` stress mode adds
extra long-tail out-of-order events and is off by default; when enabled, size
watermark/lateness separately for that stress scenario.

The event payload includes an `event_id` idempotency key (the generated
`order_id`) and `arrival_time`. A duplicate keeps the same `event_id` and
`event_time`, but gets a later `arrival_time`; the default duplicate rate is
1.5%. The default 180-day replay timeline is long enough for 7-day windows and
labels. A separate `ground_truth/` directory contains delay, burst, drift and
batch annotations for evaluation only; these flags are excluded from event
payloads. `order_month` and `product_name` are omitted because they can be
derived from `event_time` and the product dimension.

The generator also writes `labels/stream_labels_<run_id>.csv` with
`(user_id, label_date, label)`. It deduplicates by `event_id`, uses UTC dates,
and sets `label=1` when that user has a clean event in the seven days after
`label_date`. Dates without any event for that user are not emitted.

Build batch and streaming features from the same event-time definitions and
keys: `user_id`, `product_id`, and trailing 1d, 3d, and 7d windows. The generator
keeps per-user activity, product preference, reorder propensity and reorder
cycle stable across batches, and shifts product mix during the final 20% of the
timeline. These signals support learning and drift checks. Generation metadata
records the timeline, delay distribution, allowed-lateness recommendation,
keys and window definitions.

Example:

```bash
python batch_processing/dataset/gen_streaming_data.py --output batch_processing/stream_input --start-time 2025-09-01T00:00:00Z --no-sleep
```
