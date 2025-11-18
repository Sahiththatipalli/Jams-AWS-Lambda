# metrics_utils.py

import boto3
import logging
import os
import time

logger = logging.getLogger(__name__)

def publish_metric(namespace, metric_name, value, unit='Count', dimensions=None):
    """
    Publish a custom metric to CloudWatch.
    """
    cloudwatch = boto3.client('cloudwatch', region_name=os.getenv('AWS_REGION', 'us-east-1'))
    dimensions = dimensions or []
    try:
        response = cloudwatch.put_metric_data(
            Namespace=namespace,
            MetricData=[
                {
                    'MetricName': metric_name,
                    'Value': value,
                    'Unit': unit,
                    'Dimensions': dimensions
                },
            ]
        )
        logger.info(f"Published metric {metric_name}={value} ({unit}) to CloudWatch {namespace} {dimensions}")
    except Exception as e:
        logger.warning(f"Could not publish metric {metric_name}: {e}")

def publish_file_transfer_metric(namespace, direction, file_count, total_bytes, duration_sec, trace_id=None):
    dims = [
        {'Name': 'Direction', 'Value': direction},
        {'Name': 'TraceId', 'Value': trace_id or 'none'}
    ]
    publish_metric(namespace, 'FileTransferCount', file_count, 'Count', dims)
    publish_metric(namespace, 'FileTransferBytes', total_bytes, 'Bytes', dims)
    publish_metric(namespace, 'FileTransferDuration', duration_sec, 'Seconds', dims)

def publish_error_metric(namespace, error_type, trace_id=None):
    dims = [
        {'Name': 'ErrorType', 'Value': error_type},
        {'Name': 'TraceId', 'Value': trace_id or 'none'}
    ]
    publish_metric(namespace, 'FileTransferErrors', 1, 'Count', dims)

def log_metric_locally(metric_name, value, trace_id=None, **kwargs):
    """
    If you want to just log, not push to CloudWatch.
    """
    logger.info(f"[{trace_id}] Metric {metric_name}: {value} | {kwargs}")

def time_metric():
    """
    Returns a decorator for timing any function and auto-publishing a metric.
    """
    def decorator(fn):
        def wrapper(*args, **kwargs):
            t0 = time.time()
            result = fn(*args, **kwargs)
            t1 = time.time()
            duration = t1 - t0
            metric_name = f"{fn.__name__}Duration"
            publish_metric('LambdaFileTransfer', metric_name, duration, 'Seconds')
            return result
        return wrapper
    return decorator
