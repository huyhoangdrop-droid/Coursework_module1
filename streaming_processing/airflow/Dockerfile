# Custom Airflow image with Great Expectations and DataHub SDK installed.
# Build this image once; both airflow services (webserver + scheduler) use it.
FROM apache/airflow:2.10.2

USER airflow

RUN pip install --no-cache-dir \ 
    "apache-airflow==2.10.2" \
    "great_expectations==0.18.19" \
    "acryl-datahub[airflow]" \
    "acryl-datahub-airflow-plugin[airflow2]==1.6.0" \
    "pandas" \
    "sqlglot" \
    "confluent-kafka"
