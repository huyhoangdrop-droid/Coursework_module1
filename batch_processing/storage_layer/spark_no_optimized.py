"""Naive baseline used to inspect data skew in Spark UI."""

import os
from pathlib import Path

from pyspark.sql import SparkSession


DATA_DIR = Path(__file__).resolve().parents[1] / "generated_dataset"
DATA_PATH = [
    str(path) for path in sorted(DATA_DIR.glob("*.csv"))
]


# Explain config options:
# - spark.sql.adaptive.enabled: enables adaptive query execution (AQE) in Spark, which allows Spark to optimize query plans based on runtime statistics.
# - spark.sql.adaptive.skewJoin.enabled: enables skew join optimization, which automatically detects and handles skewed data during join operations by splitting skewed partitions into smaller ones.
# - spark.sql.shuffle.partitions: 
#   This sets the default number of partitions to use when shuffling data for joins or aggregations. 
#   Imagine you divided your data after a groupBy to count orders by city and category. 
#   If you have 1 million rows and set this to 8, Spark will create 8 partitions of the shuffled data. 
#   By default, this number is 200 which can lead to too many small tasks, which is not efficient because of the overhead of task scheduling and coordination.

def make_spark():
    return (
        SparkSession.builder
        .master("local[4]")
        .appName("instacart_naive_baseline")
        .config("spark.sql.adaptive.enabled", "false") #AQE = false
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.port", "3636")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Approach 1 - Naive baseline
# - Spark infers the CSV schema.
# - HCMC contains most rows, but GROUP BY is performed directly without salt.
# - COUNT(DISTINCT user_id) creates a large shuffle for a high-cardinality key.
# - AQE is disabled, so the problem remains visible in Spark UI.
# ---------------------------------------------------------------------------
def run_naive(spark):
    orders = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(DATA_PATH)
    )
    orders.createOrReplaceTempView("orders")

    return spark.sql("""
        SELECT
            city,
            order_month,
            COUNT(order_id)          AS num_orders,
            COUNT(DISTINCT user_id)  AS unique_users,
            SUM(amount)              AS total_amount
        FROM orders
        GROUP BY city, order_month
        ORDER BY num_orders DESC
    """)

if __name__ == "__main__":
    spark = make_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading {len(DATA_PATH)} CSV files")
    print("Spark UI: http://localhost:3636")
    run_naive(spark).show(100, truncate=False)

    input("Press Enter after inspecting Spark UI...")
    spark.stop()
