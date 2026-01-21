import boto3
import requests
from datetime import datetime, timedelta
import time
import os
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info("--NYC Taxi Data Ingestion--")
    logger.info(f"Event: {event}")
    logger.info(f"Context: {context}")

    s3_client = boto3.client('s3')
    cloudwatch = boto3.client('cloudwatch')
    
    s3_bucket = os.getenv('S3_BUCKET') 
    s3_prefix = os.getenv('S3_PREFIX') 
    logger.info(f"Configuration - S3 Bucket: {s3_bucket}, Prefix: {s3_prefix}")
    
    # Find the previous month
    now = datetime.now()
    target_date = now - timedelta(days=180)
    year = str(target_date.year)
    month = str(target_date.month).zfill(2)
    logger.info(f"Target month: {year}-{month}")

    try:
        # Check if already exists
        if file_exists_in_s3(s3_client, s3_bucket, s3_prefix, year, month):
            logger.warning(f"Data for {year}-{month} already exists in S3. Skipping download.")
            put_skipped_metric(cloudwatch, year, month) 
            return {
                'statusCode': 200,
                'body': f"Data already exists for {year}-{month}"
            }
        
        # Try to download
        if process_month(s3_client, cloudwatch, s3_bucket, s3_prefix, year, month):
            logger.info(f"Successfully processed {year}-{month}")
            put_success_metric(cloudwatch, year, month)
            return {
                'statusCode': 200,
                'body': f"Successfully processed {year}-{month}"
            }
        
    except Exception as e:
        logger.error(f"Unexpected error in Lambda handler: {str(e)}", exc_info=True)
        put_failure_metric(cloudwatch)
        return {
            'statusCode': 500,
            'body': f"Unexpected error: {str(e)}"
        }

def file_exists_in_s3(s3_client, bucket, prefix, year, month):
    """Check if file already exists in S3"""
    file_name = f"yellow_tripdata_{year}-{month}.parquet"
    s3_key = f"{prefix}year={year}/month={month}/{file_name}"
    
    try:
        s3_client.head_object(Bucket=bucket, Key=s3_key)
        print(f"File already exists: s3://{bucket}/{s3_key}")
        return True
    except s3_client.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            # Other error (permissions, etc.)
            print(f"Error checking S3: {e}")
            return False

def process_month(s3_client, cloudwatch, bucket, prefix, year, month):
    file_name = f"yellow_tripdata_{year}-{month}.parquet"

    # Source bucket (where the file already is)
    source_bucket = "bucket-day1-oubt"
    source_key = file_name  # adjust if it’s in a folder

    # Destination bucket/prefix (your data lake target)
    dest_bucket = bucket
    dest_key = f"{prefix}year={year}/month={month}/{file_name}"

    try:
        start_time = time.time()

        # Server-side S3 copy (no HTTP, uses Lambda role permissions)
        s3_client.copy_object(
            Bucket=dest_bucket,
            Key=dest_key,
            CopySource={"Bucket": source_bucket, "Key": source_key}
        )

        copy_duration = time.time() - start_time
        logger.info(f"Copied s3://{source_bucket}/{source_key} to s3://{dest_bucket}/{dest_key} in {copy_duration:.2f}s")

        # You can still send metrics, using copy_duration as download_duration
        send_performance_metrics(
            cloudwatch,
            year,
            month,
            download_duration=copy_duration,
            upload_duration=0,
            file_size_bytes=0,  # or head_object to get ContentLength
            success=True
        )

        return True

    except Exception as e:
        logger.error(f"Failed copy for {file_name}: {e}")
        send_performance_metrics(
            cloudwatch,
            year,
            month,
            download_duration=0,
            upload_duration=0,
            file_size_bytes=0,
            success=False
        )
        return False


def send_performance_metrics(cloudwatch, year, month, download_duration, upload_duration, file_size_bytes, success):
    """Send detailed performance metrics to CloudWatch"""
    try:
        metric_data = []
        
        # Download duration metric
        metric_data.append({
            'MetricName': 'DownloadDuration',
            'Dimensions': [
                {'Name': 'Year', 'Value': year},
                {'Name': 'Month', 'Value': month},
                {'Name': 'Status', 'Value': 'Success' if success else 'Failure'}
            ],
            'Value': download_duration,
            'Unit': 'Seconds'
        })
        
        # Upload duration metric (only for successful downloads)
        if success and upload_duration > 0:
            metric_data.append({
                'MetricName': 'UploadDuration',
                'Dimensions': [
                    {'Name': 'Year', 'Value': year},
                    {'Name': 'Month', 'Value': month}
                ],
                'Value': upload_duration,
                'Unit': 'Seconds'
            })
        
        # File size metric (only for successful downloads)
        if success and file_size_bytes > 0:
            metric_data.append({
                'MetricName': 'FileSize',
                'Dimensions': [
                    {'Name': 'Year', 'Value': year},
                    {'Name': 'Month', 'Value': month}
                ],
                'Value': file_size_bytes,
                'Unit': 'Bytes'
            })
            
            # Also log in MB for easier reading
            metric_data.append({
                'MetricName': 'FileSizeMB',
                'Dimensions': [
                    {'Name': 'Year', 'Value': year},
                    {'Name': 'Month', 'Value': month}
                ],
                'Value': file_size_bytes / (1024 * 1024),
                'Unit': 'Megabytes'
            })
        
        # Throughput metric (MB/s)
        if success and download_duration > 0 and file_size_bytes > 0:
            throughput = file_size_bytes / download_duration / (1024 * 1024)  # MB/s
            metric_data.append({
                'MetricName': 'DownloadThroughput',
                'Dimensions': [
                    {'Name': 'Year', 'Value': year},
                    {'Name': 'Month', 'Value': month}
                ],
                'Value': throughput,
                'Unit': 'Megabytes/Second'
            })
        
        # Send all metrics in one call (more efficient)
        if metric_data:
            cloudwatch.put_metric_data(
                Namespace='NYCTaxiDownload',
                MetricData=metric_data
            )
            logger.debug(f"Sent {len(metric_data)} performance metrics to CloudWatch")
            
    except Exception as e:
        logger.error(f"Failed to send performance metrics to CloudWatch: {e}")

def put_success_metric(cloudwatch, year, month):
    """Record overall job success"""
    try:
        cloudwatch.put_metric_data(
            Namespace='NYCTaxiDownload',
            MetricData=[{
                'MetricName': 'JobSuccess',
                'Dimensions': [
                    {'Name': 'Year', 'Value': year},
                    {'Name': 'Month', 'Value': month}
                ],
                'Value': 1.0,
                'Unit': 'Count'
            }]
        )
        logger.info(f"Recorded job success for {year}-{month}")
    except Exception as e:
        logger.error(f"Failed to record success metric: {e}")

def put_failure_metric(cloudwatch, year, month):
    """Record overall job failure"""
    try:
        cloudwatch.put_metric_data(
            Namespace='NYCTaxiDownload',
            MetricData=[{
                'MetricName': 'JobFailure', 
                'Dimensions': [
                    {'Name': 'Year', 'Value': year},
                    {'Name': 'Month', 'Value': month}
                ],
                'Value': 1.0,
                'Unit': 'Count'
            }]
        )
        logger.error(f"Recorded job failure for {year}-{month}")
    except Exception as e:
        logger.error(f"Failed to record failure metric: {e}")

def put_skipped_metric(cloudwatch, year, month):
    """Record skipped download (already exists)"""
    try:
        cloudwatch.put_metric_data(
            Namespace='NYCTaxiDownload',
            MetricData=[{
                'MetricName': 'JobSkipped',
                'Dimensions': [
                    {'Name': 'Year', 'Value': year},
                    {'Name': 'Month', 'Value': month}
                ],
                'Value': 1.0,
                'Unit': 'Count'
            }]
        )
        logger.info(f"Recorded job skipped for {year}-{month}")
    except Exception as e:
        logger.error(f"Failed to record skipped metric: {e}")
