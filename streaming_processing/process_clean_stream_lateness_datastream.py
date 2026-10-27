"""Aggregate CDC events in event-time windows with allowed lateness."""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyflink.common import Duration, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction
from pyflink.datastream.window import Time, TumblingEventTimeWindows


KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_SOURCE_TOPIC = os.getenv("KAFKA_SOURCE_TOPIC", "dbserver1.public.stream_events")
KAFKA_SINK_TOPIC = os.getenv("KAFKA_SINK_TOPIC", "orders_aggregated_late")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "flink-stream-events-lateness-v1")
WINDOW_MIN = int(os.getenv("WINDOW_MIN", "1"))
OUT_OF_ORDERNESS_SEC = int(os.getenv("OUT_OF_ORDERNESS_SEC", "5"))
ALLOWED_LATENESS_SEC = int(os.getenv("ALLOWED_LATENESS_SEC", "900"))

'''must add try except to track(provide later)'''
def parse_order(raw):
    """load data from cdc."""
    event_data = json.loads(raw)["payload"]["after"]["event_data"]
    event = json.loads(event_data)
    return (event["city"], event["category"], int(event["user_id"]),
            float(event["amount"]))


class OrderTimestampAssigner(TimestampAssigner):
    """Use event_timestamp rather than the time Kafka received the order."""

    def extract_timestamp(self, raw, record_timestamp):
        if raw is None:
            return record_timestamp
        record = json.loads(raw)
        payload = record.get("payload", record)
        row = payload.get("after", payload)
        event_time = row.get("event_time") if row else None
        if not event_time and row:
            event = row.get("event_data", row)
            if isinstance(event, str):
                event = json.loads(event)
            event_time = event.get("event_time")
        if not event_time:
            return record_timestamp
        timestamp = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        return int(timestamp.astimezone(timezone.utc).timestamp() * 60)


class OrderAggFunction(AggregateFunction):
    def create_accumulator(self):
        return {"count": 0, "total": 0.0, "customers": set()}

    def add(self, order, accumulator):
        accumulator["count"] += 1
        accumulator["total"] += order[3]
        accumulator["customers"].add(order[2])
        return accumulator

    def get_result(self, accumulator):
        count = accumulator["count"]
        total = accumulator["total"]
        average = round(total / count, 2) if count else 0.0
        return count, average, round(total, 2), len(accumulator["customers"])

    def merge(self, left, right):
        left["count"] += right["count"]
        left["total"] += right["total"]
        left["customers"].update(right["customers"])
        return left


class WindowResultFunction(ProcessWindowFunction):
    def process(self, key, context, aggregates):
        count, average, total, unique_customers = list(aggregates)[0]
        window = context.window()
        start = datetime.fromtimestamp(window.start / 1000, timezone.utc).isoformat()
        end = datetime.fromtimestamp(window.end / 1000, timezone.utc).isoformat()
        yield json.dumps({
            "city": key[0], "category": key[1],
            "window_start": start, "window_end": end,
            "num_events": count, "avg_amount": average,
            "total_amount": total, "unique_users": unique_customers,
        })


def build_pipeline():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # as_uri() formats Windows paths as file:///C:/... for the Java gateway.
    jar = Path(__file__).resolve().parent / "jars" / "flink-sql-connector-kafka-4.0.1-2.0.jar"
    env.add_jars(jar.as_uri())

    source = (KafkaSource.builder()
              .set_bootstrap_servers(KAFKA_BROKER)
              .set_topics(KAFKA_SOURCE_TOPIC)
              .set_group_id(KAFKA_GROUP_ID)
              .set_starting_offsets(KafkaOffsetsInitializer.earliest())
              # Replay current records and emit a final watermark at the end.
              
              .set_bounded(KafkaOffsetsInitializer.latest()) #get the only message earliest -> latest message then automatic end
              .set_value_only_deserializer(SimpleStringSchema())
              .build())

    watermarks = (WatermarkStrategy
                  .for_bounded_out_of_orderness(
                      Duration.of_seconds(OUT_OF_ORDERNESS_SEC))
                  .with_timestamp_assigner(OrderTimestampAssigner()))
    raw_stream = env.from_source(source, watermarks, "Kafka CDC stream_events")

    result = (raw_stream
              .filter(lambda raw: raw is not None)
              .map(parse_order, output_type=Types.TUPLE([
                  Types.STRING(), Types.STRING(), Types.LONG(), Types.DOUBLE()]))
              .key_by(lambda order: (order[0], order[1]))
              .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_MIN)))
              # Keep the closed window's state so late orders can update its result.
              .allowed_lateness(ALLOWED_LATENESS_SEC * 1000)
              .aggregate(OrderAggFunction(), WindowResultFunction(),
                         output_type=Types.STRING()))

    sink = (KafkaSink.builder()
            .set_bootstrap_servers(KAFKA_BROKER)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(KAFKA_SINK_TOPIC)
                .set_value_serialization_schema(SimpleStringSchema())
                .build())
            .build())
    result.sink_to(sink)
    return env


if __name__ == "__main__":
    build_pipeline().execute("orders-window-aggregation-with-allowed-lateness")
