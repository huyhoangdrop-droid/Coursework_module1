
from __future__ import annotations

import argparse
import os
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

BATCH_DIR = Path(__file__).resolve().parent
DEFAULT_SOURCE = BATCH_DIR / "generated_dataset"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--bucket", default=os.getenv("S3_BUCKET", "department-data"))
    parser.add_argument("--prefix", default=os.getenv("S3_PREFIX", "raw/instacart"))
    parser.add_argument("--endpoint", default=os.getenv("S3_ENDPOINT", "http://localhost:9000"))
    return parser.parse_args()


def make_client(endpoint: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "seaweedfs"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "seaweedfs"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        config=Config(s3={"addressing_style": "path"}),
    )

#make sure that bucket available
def ensure_bucket(s3, bucket: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as error:
        status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status not in (403, 404):
            raise
        s3.create_bucket(Bucket=bucket)


def main() -> None:
    args = parse_args()
    source = args.source.resolve()
    files = sorted(source.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV files found in {source}")

    s3 = make_client(args.endpoint)
    ensure_bucket(s3, args.bucket)
    prefix = args.prefix.strip("/")
    # Synchronize the current source snapshot so stale CSV parts from an older,
    # larger generation are not ingested accidentally.
    expected_keys = {f"{prefix}/{path.name}" for path in files}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=args.bucket, Prefix=f"{prefix}/"):
        stale = [
            {"Key": item["Key"]}
            for item in page.get("Contents", [])
            if item["Key"].endswith(".csv") and item["Key"] not in expected_keys
        ]
        if stale:
            s3.delete_objects(Bucket=args.bucket, Delete={"Objects": stale})
            print(f"Removed {len(stale)} stale CSV object(s)")

    for path in files:
        key = f"{prefix}/{path.name}"
        s3.upload_file(str(path), args.bucket, key)
        print(f"Uploaded {path.name} -> s3://{args.bucket}/{key}")

    report = source / "generation_report.json"
    if report.exists():
        key = f"{prefix}/_metadata/{report.name}"
        s3.upload_file(str(report), args.bucket, key)
        print(f"Uploaded {report.name} -> s3://{args.bucket}/{key}")
    print(f"Landing zone ready: s3://{args.bucket}/{prefix}/")


if __name__ == "__main__":
    main()
