#!/usr/bin/env bash
# Submit a PyFlink job to the Flink cluster running in docker compose.
#
# Usage:
#   ./run_on_flink_cluster.sh                                   # submit 1_process_clean_stream_datastream.py
#   ./run_on_flink_cluster.sh optional/3_process_clean_stream_sql.py
#
# Flink UI: http://localhost:8881
set -euo pipefail
cd "$(dirname "$0")"

JOB_FILE="${1:-process_clean_stream_datastream.py}"

# Build the PyFlink image only if it does not exist yet (rebuild manually with: docker compose build jobmanager).
# Without --build, `up -d` leaves running containers alone, so jobs already on the cluster keep running.
if ! docker image inspect pyflink:2.0 >/dev/null 2>&1; then
  docker compose build jobmanager
fi
docker compose up -d jobmanager taskmanager

echo "Waiting for JobManager REST API..."

#if window 
until curl -sf http://localhost:8881/overview >/dev/null; do sleep 2; done
# -d: detached, the job keeps running on the cluster after this script exits
# -pyexec: Python interpreter used by the Python UDF workers on the TaskManager
docker compose exec -w /opt/job jobmanager \
  flink run -d \
    -pyexec python3 \
    -py "/opt/job/${JOB_FILE}"

echo
echo "Submitted. Check http://localhost:8881 (Running Jobs) and TaskManager stdout for output."
