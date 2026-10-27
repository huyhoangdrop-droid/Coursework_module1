from __future__ import annotations
import json
from datetime import datetime, timezone
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datahub_airflow_plugin.entities import Dataset as DatahubDataset

DB_CONN = "postgresql+psycopg2://airflow:airflow@postgres/airflow"
DATAHUB_GMS_URL = "http://datahub-gms:8080"
KAFKA_BROKER = "kafka:19092"
KAFKA_SINK_TOPIC = "orders_aggregated_late"

# define URN for Kafka Topic (output of Flink) and Gold in Postgres
KAFKA_TOPIC_URN = f"urn:li:dataset:(urn:li:dataPlatform:kafka,{KAFKA_SINK_TOPIC},PROD)"
GOLD_TABLE_URN = "urn:li:dataset:(urn:li:dataPlatform:postgres,airflow.public.gold_orders_aggregated,PROD)"


def consume_flink_sink(**context) -> None:
    """1. get result from topic orders_aggregated_late of Flink."""
    from confluent_kafka import Consumer

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "airflow-gold-loader-v1",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([KAFKA_SINK_TOPIC])

    records = []
    # max 5000 events
    while len(records) < 5000:
        msg = consumer.poll(timeout=2.0)
        if msg is None:
            break
        if not msg.error():
            records.append(json.loads(msg.value().decode("utf-8")))

    consumer.close()
    print(f"[consume_flink_sink] was got {len(records)} from {KAFKA_SINK_TOPIC}")
    context["ti"].xcom_push(key="flink_window_data", value=json.dumps(records))


def validate_stream_quality(**context) -> None:
    """2. use Great Expectations check output of Flink and send Assertion to DataHub."""
    import great_expectations as ge
    from datahub.emitter.rest_emitter import DataHubRestEmitter
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import (
        AssertionInfoClass, AssertionTypeClass,
        AssertionResultClass, AssertionResultTypeClass,
        AssertionRunEventClass, AssertionRunStatusClass,
        DatasetAssertionInfoClass, DatasetAssertionScopeClass,
        AssertionStdOperatorClass,
    )

    raw_json = context["ti"].xcom_pull(key="flink_window_data")
    records = json.loads(raw_json) if raw_json else []
    if not records:
        print("[validate_stream_quality] have no new event in this window-> skip.")
        return

    df = pd.DataFrame(records)
    ge_df = ge.from_pandas(df)

    ge_df.expect_column_values_to_not_be_null("city")
    ge_df.expect_column_values_to_not_be_null("category")
    ge_df.expect_column_values_to_be_between("num_events", min_value=1)
    ge_df.expect_column_values_to_be_between("total_amount", min_value=0.0)
    ge_df.expect_column_values_to_be_between("unique_users", min_value=1)

    results = ge_df.validate()
    emitter = DataHubRestEmitter(DATAHUB_GMS_URL)
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    run_id = f"stream_orders_{now_ms}"
    # Loop over each expectation result and emit an assertion info and assertion run event to DataHub.
    # This allows us to track the quality of our dataset in DataHub.
    for result in results["results"]:
        # Get the expectation type and column (if applicable) to construct a unique assertion URN.
        exp_type = result["expectation_config"]["expectation_type"]
        col = result["expectation_config"].get("kwargs", {}).get("column", "")
        # Assertion is uniquely identified by the combination of expectation type and column (if applicable)
        assertion_urn = f"urn:li:assertion:flink_stream.{exp_type}.{col}"
        
        emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=assertion_urn,
            aspect=AssertionInfoClass(
                type=AssertionTypeClass.DATASET,
                datasetAssertion=DatasetAssertionInfoClass(
                    dataset=KAFKA_TOPIC_URN,
                    scope=DatasetAssertionScopeClass.DATASET_COLUMN,
                    fields=[f"urn:li:schemaField:({KAFKA_TOPIC_URN},{col})"],
                    operator=AssertionStdOperatorClass._NATIVE_,
                    nativeType=exp_type,
                ),
            ),
        ))
        emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=assertion_urn,
            aspect=AssertionRunEventClass(
                timestampMillis=now_ms,
                asserteeUrn=KAFKA_TOPIC_URN,
                runId=run_id,
                assertionUrn=assertion_urn,
                status=AssertionRunStatusClass.COMPLETE,
                result=AssertionResultClass(
                    type=AssertionResultTypeClass.SUCCESS if result["success"] else AssertionResultTypeClass.FAILURE,
                    nativeResults={"success": str(result["success"]), "expectation_type": exp_type},
                ),
            ),
        ))

    if not results["success"]:
        raise ValueError("data from kafka sink is not great enough!")


def upsert_to_gold_and_emit_lineage(**context) -> None:
    """3. Upsert vào bảng Staging/Gold (xử lý cập nhật từ allowed_lateness) và bắn Lineage."""
    from sqlalchemy import create_engine, text
    from datahub.emitter.rest_emitter import DataHubRestEmitter
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import (
        UpstreamLineageClass, UpstreamClass, AuditStampClass,
    )

    raw_json = context["ti"].xcom_pull(key="flink_window_data")
    records = json.loads(raw_json) if raw_json else []
    if not records:
        return

    # Khử trùng lặp ngay trong lô nếu có late event cập nhật cùng 1 cửa sổ (lấy bản ghi mới nhất ở cuối)
    df = pd.DataFrame(records).drop_duplicates(
        subset=["city", "category", "window_start", "window_end"], keep="last"
    )

    engine = create_engine(DB_CONN)
    upsert_sql = text("""
        INSERT INTO gold_orders_aggregated 
            (city, category, window_start, window_end, num_events, avg_amount, total_amount, unique_users, updated_at)
        VALUES 
            (:city, :category, :window_start, :window_end, :num_events, :avg_amount, :total_amount, :unique_users, NOW())
        ON CONFLICT (city, category, window_start, window_end)
        DO UPDATE SET
            num_events = EXCLUDED.num_events,
            avg_amount = EXCLUDED.avg_amount,
            total_amount = EXCLUDED.total_amount,
            unique_users = EXCLUDED.unique_users,
            updated_at = NOW();
    """)

    with engine.begin() as conn:
        conn.execute(upsert_sql, df.to_dict(orient="records"))

    # Bắn Lineage lên DataHub: Kafka Topic (orders_aggregated_late) -> Postgres (gold_orders_aggregated)
    emitter = DataHubRestEmitter(DATAHUB_GMS_URL)
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    emitter.emit(MetadataChangeProposalWrapper(
        entityUrn=GOLD_TABLE_URN,
        aspect=UpstreamLineageClass(
            upstreams=[
                UpstreamClass(
                    dataset=KAFKA_TOPIC_URN,
                    auditStamp=AuditStampClass(time=now_ms, actor="urn:li:corpuser:airflow"),
                )
            ]
        ),
    ))


with DAG(
    dag_id="stream_orders_to_gold_pipeline",
    description="Consume Flink Kafka Sink => Validate with GX => Upsert to Gold Postgres => Emit DataHub Lineage",
    start_date=datetime(2026, 1, 1),
    schedule_interval="*/5 * * * *",  # Chạy mỗi 5 phút
    catchup=False,
    max_active_runs=1,
    tags=["flink", "kafka", "great-expectations", "datahub"],
) as dag:

    t_consume = PythonOperator(
        task_id="consume_flink_sink",
        python_callable=consume_flink_sink,
        inlets=[DatahubDataset("kafka", KAFKA_SINK_TOPIC)],
    )
    t_validate = PythonOperator(
        task_id="validate_stream_quality",
        python_callable=validate_stream_quality,
    )
    t_upsert = PythonOperator(
        task_id="upsert_to_gold",
        python_callable=upsert_to_gold_and_emit_lineage,
        outlets=[DatahubDataset("postgres", "airflow.public.gold_orders_aggregated")],
    )

    t_consume >> t_validate >> t_upsert