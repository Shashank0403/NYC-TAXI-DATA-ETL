import json
import boto3
import re
import logging
from datetime import datetime
from urllib.parse import unquote_plus

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event, default=str)}")

    try:
        cloudwatch = boto3.client('cloudwatch')

        bucket, key = extract_bucket_key(event)

        if not bucket or not key:
            logger.error("Unable to extract bucket and key")
            return {
                "statusCode": 400,
                "body": json.dumps("Invalid S3 event structure")
            }

        logger.info(f"Processing file: s3://{bucket}/{key}")

        # Validate Yellow Taxi parquet file
        if "yellow_tripdata" not in key or not key.endswith(".parquet"):
            logger.info("Not a Yellow Taxi parquet file. Skipping.")
            return {
                "statusCode": 200,
                "body": json.dumps("Skipped non-yellow taxi file")
            }

        # Extract year/month
        year_match = re.search(r"year=(\d{4})", key)
        month_match = re.search(r"month=(\d{2})", key)

        if not year_match or not month_match:
            logger.error("Year/month missing in S3 key")
            return {
                "statusCode": 400,
                "body": json.dumps("Year or month not found in S3 key")
            }

        year = year_match.group(1)
        month = month_match.group(1)

        logger.info(f"Validated Yellow Taxi file for {year}-{month}")

        send_cloudwatch_metric(
            cloudwatch,
            "YellowTaxiFileValidated",
            1,
            [
                {"Name": "Year", "Value": year},
                {"Name": "Month", "Value": month}
            ]
        )

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Yellow taxi file validated",
                "year": year,
                "month": month,
                "s3_path": f"s3://{bucket}/{key}"
            })
        }

    except Exception as e:
        logger.error(f"Unhandled error: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps(str(e))
        }

def extract_bucket_key(event):
    """
    Supports:
    - S3 -> Lambda notifications
    - EventBridge S3 events
    """

    # Case 1: S3 Notification
    if "Records" in event:
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])
        return bucket, key

    # Case 2: EventBridge
    detail = event.get("detail", {})
    bucket = (
        detail.get("bucket", {}).get("name") or
        detail.get("requestParameters", {}).get("bucketName")
    )
    key = (
        detail.get("object", {}).get("key") or
        detail.get("requestParameters", {}).get("key")
    )

    return bucket, key

def send_cloudwatch_metric(cloudwatch, name, value, dimensions):
    cloudwatch.put_metric_data(
        Namespace="NYCTaxiProcessing",
        MetricData=[{
            "MetricName": name,
            "Value": value,
            "Unit": "Count",
            "Dimensions": dimensions,
            "Timestamp": datetime.utcnow()
        }]
    )
