
# Mini coursework: CSV -> SeaweedFS -> PySpark -> PostgreSQL

Pipeline mô phỏng dữ liệu CSV nằm ở một department khác, đưa raw data vào
SeaweedFS, xử lý bằng PySpark và nạp kết quả vào PostgreSQL data warehouse.

## Kiến trúc

```text
generated_dataset/*.csv
        | boto3 / S3 API
        v
SeaweedFS: s3://department-data/raw/instacart/
        | Spark S3A
        v
PySpark: schema evolution -> validate -> deduplicate
         -> AQE + aggregation
        | JDBC
        v
PostgreSQL (database `metastore`, schema `warehouse`): warehouse.fact_orders
            warehouse.dim_product
            warehouse.city_month_summary
            warehouse.pipeline_quality_metrics
```

SeaweedFS là landing/raw zone. CSV được giữ nguyên nội dung để có thể ingest
lại hoặc dùng làm đầu vào cho Bronze zone sau này; uploader đồng bộ prefix hiện
tại để không đọc nhầm các part cũ sau khi regenerate dataset nhỏ hơn.

## Chạy nhanh trên Windows PowerShell

Yêu cầu: Docker Desktop, Java và Python virtual environment đã cài
`requirements.txt`.

```powershell
docker compose -f batch_processing/storage_layer/docker-compose.yaml up -d
.\.venv\Scripts\python.exe batch_processing\upload_to_seaweedfs.py
.\.venv\Scripts\python.exe batch_processing\storage_layer\spark_optimized.py
```

Job mặc định dùng AQE và 8 shuffle partitions, phù hợp bộ dữ liệu demo khoảng
150 MB. Có thể đổi bằng biến môi trường `SPARK_PARTITIONS` nếu cần.

Các endpoint:

- SeaweedFS S3 API: `http://localhost:9000`
- SeaweedFS Master UI: `http://localhost:9333`
- SeaweedFS Filer UI: `http://localhost:8888`
- PostgreSQL: `localhost:5432`, database/user/password = `metastore/hive/hive`

Kiểm tra warehouse:

```powershell
docker exec metastore-db psql -U hive -d metastore -c `
  "SELECT * FROM warehouse.pipeline_quality_metrics ORDER BY metric_name;"
```

Dừng service (data vẫn được giữ trong Docker volumes):

```powershell
docker compose -f batch_processing/storage_layer/docker-compose.yaml down
```

## Các vấn đề dữ liệu được xử lý

- **Schema evolution:** file schema v1 thiếu `coupon_code` và `shipping_method`;
  schema cố định đưa hai cột này về `UNKNOWN`.
- **Duplicate:** loại trùng theo business key `order_id`.
- **Skew:** `HCMC` chiếm phần lớn dữ liệu, nhưng ở quy mô demo khoảng 150 MB thì
  `groupBy` trực tiếp là đủ; AQE gộp các shuffle partition nhỏ ở runtime.
- **High cardinality:** không partition theo `user_id`; fact table giữ user key.
  Product attributes được tách thành dimension.
- **Data quality:** loại bản ghi thiếu key hoặc tháng không hợp lệ và ghi số liệu
  kiểm soát vào `warehouse.pipeline_quality_metrics`.

Pipeline ghi đè từng bảng bằng JDBC nên chạy lại không nhân đôi dữ liệu. Với
production incremental load, nên ghi vào staging table rồi `MERGE` theo `order_id`.

## Streaming: PostgreSQL → Debezium → Kafka → Flink → Kafka

### Replay streaming events through CDC

The streaming replay uses generated `events_*.json` JSONL files only. It does not
read the sibling `ground_truth/` or `labels/` directories, and removes label,
ground-truth, and diagnostic fields before writing events to PostgreSQL.
`streaming_processing/ingest_stream_to_postgres.py` upserts each event into
`public.stream_events` using `event_id` as the key. Debezium publishes changes to
`dbserver1.public.stream_events`; the PyFlink job reads that topic, aggregates
event count and amount by city/category in event-time windows, and writes JSON to
`stream_events.aggregated`.

From the repository root, start the existing stack and then replay the dataset:

```powershell
cd streaming_processing
docker compose up -d --build
cd ..
python -m pip install "psycopg[binary]" "apache-flink==1.19.0"
python streaming_processing/ingest_stream_to_postgres.py --input batch_processing/stream_input --interval-seconds 1
```

To continuously watch for newly generated files, add `--replay`. Generate them
in another terminal with `python batch_processing/dataset/gen_streaming_data.py
--output batch_processing/stream_input`. Set `KAFKA_BROKER=localhost:9092` and
run `python streaming_processing/process_clean_stream_lateness_datastream.py`
from the repository root in a PyFlink 1.19 environment. The Kafka connector JAR
is included under `streaming_processing/jars`. Set `KAFKA_SINK_TOPIC` to change
the output topic. The source starts at the earliest retained CDC record.

For an already initialized PostgreSQL volume, the init SQL mounted by Compose
does not run again; the ingestion script creates the table and publication on
startup. If the connector was registered before that first ingestion, restart
it after the table exists with:

```powershell
docker compose -f streaming_processing/docker-compose.yaml restart debezium
```

Ba Compose file trong `streaming_processing` dùng chung Docker network
`streaming_pipeline`. Chạy từ thư mục đó theo thứ tự sau trong PowerShell:

```powershell
cd streaming_processing
docker compose -f docker-compose.yaml up -d --build
docker compose -f datahub-docker-compose.yaml --profile quickstart up -d
docker compose -f airflow-docker-compose.yaml up -d --build
```

Compose chính khởi động PostgreSQL, Kafka, Kafka Connect/Debezium và Flink. Nó
cũng tự đăng ký cấu hình `cdc_needed/connector-config.json` và submit job Flink
đọc topic `dbserver1.public.users2`. Job kiểm tra Debezium envelope, ghi event
hợp lệ vào `users2.validated`, event lỗi vào `users2.rejected`. Phần kiểm tra
hiện tại xác minh cấu trúc CDC; hãy thêm rule nghiệp vụ theo schema thật của
bảng `users2` trong `flink/src/main/java/coursework/FlinkValidationJob.java`.

Airflow DAG `postgres_debezium_flink_kafka` kiểm tra connector và Flink, đồng
thời khai báo lineage Postgres → topic CDC → hai topic kết quả để DataHub hiển
thị pipeline và trạng thái chạy. Mở Airflow tại `http://localhost:8082`, chạy
DAG thủ công; DataHub tại `http://localhost:9002` (đăng nhập mặc định quickstart
nếu UI yêu cầu).

Tạo bảng demo trước khi gửi dữ liệu (hoặc thay bằng schema của bạn):

```powershell
docker compose -f docker-compose.yaml exec postgres psql -U postgres -d testdb -c `
  "CREATE TABLE IF NOT EXISTS public.users2 (id BIGINT PRIMARY KEY, name TEXT, email TEXT);"
docker compose -f docker-compose.yaml exec postgres psql -U postgres -d testdb -c `
  "INSERT INTO public.users2 VALUES (1, 'An', 'an@example.com') ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name, email=EXCLUDED.email;"
```

Các endpoint từ host: Postgres `localhost:5432`, Kafka `localhost:19092`,
Kafka Connect `localhost:8083`, Flink `localhost:8081`, Airflow `localhost:8082`,
DataHub `localhost:9002`. Cấu hình mẫu dùng thông tin đăng nhập local/demo; hãy
đổi chúng trước khi dùng ngoài máy cá nhân. Dừng các stack mà vẫn giữ data:

```powershell
docker compose -f airflow-docker-compose.yaml down
docker compose -f datahub-docker-compose.yaml --profile quickstart down
docker compose -f docker-compose.yaml down
```

create network docker: docker network create kafka-net
