# metrics_utils.py  (safe/no-throw version)
import os
import logging
import boto3
from botocore.exceptions import ClientError

cloudwatch = boto3.client('cloudwatch')
log = logging.getLogger(__name__)

DEFAULT_NAMESPACE = os.getenv("METRICS_NAMESPACE", "LambdaFileTransfer")

_TRUTHY = {"1","true","t","yes","y","on"}
def _enabled() -> bool:
    return str(os.getenv("METRICS_ENABLED", "true")).strip().lower() in _TRUTHY

def _put_metric_data_safe(namespace: str, metric_data: list) -> bool:
    if not _enabled():
        return False
    try:
        cloudwatch.put_metric_data(Namespace=namespace, MetricData=metric_data)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        log.warning(f"CloudWatch PutMetricData failed ({code}); continuing without metrics. {e}")
        return False
    except Exception as e:
        log.warning(f"CloudWatch PutMetricData failed; continuing without metrics. {e}")
        return False

def publish_file_transfer_metric(namespace, direction, file_count, total_bytes, duration_sec, trace_id):
    ns = namespace or DEFAULT_NAMESPACE
    md = [
        {
            "MetricName": "FileTransferCount",
            "Value": file_count,
            "Unit": "Count",
            "Dimensions": [
                {"Name": "Direction", "Value": direction},
                {"Name": "TraceId", "Value": trace_id},
            ],
        },
        {
            "MetricName": "FileTransferBytes",
            "Value": total_bytes,
            "Unit": "Bytes",
            "Dimensions": [
                {"Name": "Direction", "Value": direction},
                {"Name": "TraceId", "Value": trace_id},
            ],
        },
        {
            "MetricName": "FileTransferDuration",
            "Value": duration_sec,
            "Unit": "Seconds",
            "Dimensions": [
                {"Name": "Direction", "Value": direction},
                {"Name": "TraceId", "Value": trace_id},
            ],
        },
    ]
    _put_metric_data_safe(ns, md)

def publish_error_metric(namespace, error_type, trace_id):
    ns = namespace or DEFAULT_NAMESPACE
    md = [
        {
            "MetricName": "Error",
            "Value": 1,
            "Unit": "Count",
            "Dimensions": [
                {"Name": "ErrorType", "Value": error_type},
                {"Name": "TraceId", "Value": trace_id},
            ],
        }
    ]
    _put_metric_data_safe(ns, md)
