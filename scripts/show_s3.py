"""Print what is stored in the raw-data bucket: objects per dataset with sizes.

Runs inside the Airflow container (boto3 and the S3_* variables are there):
    docker compose exec -T airflow python /opt/airflow/repo/scripts/show_s3.py
"""

import os
from collections import defaultdict

import boto3


def main() -> None:
    client = boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("S3_SECRET_KEY", "minioadmin"),
    )
    bucket = os.environ.get("S3_BUCKET", "raw-data")
    stats: dict[str, dict[str, int]] = defaultdict(lambda: {"batches": 0, "bytes": 0, "run manifests": 0})
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            dataset, _, name = obj["Key"].partition("/")
            if name.endswith(".jsonl"):
                stats[dataset]["batches"] += 1
                stats[dataset]["bytes"] += obj["Size"]
            elif name.startswith("runs/"):
                stats[dataset]["run manifests"] += 1

    print(f"bucket s3://{bucket}")
    print(f"{'dataset':<16}{'batches':>8}{'KiB':>8}{'run manifests':>15}")
    for dataset, values in sorted(stats.items()):
        print(f"{dataset:<16}{values['batches']:>8}{values['bytes'] / 1024:>8.1f}{values['run manifests']:>15}")


if __name__ == "__main__":
    main()
