import boto3

cloudwatch = boto3.client("cloudwatch")

def publish_file_transfer_metric(namespace, direction, file_count, total_bytes, duration_sec, trace_id):
    cloudwatch.put_metric_data(
        Namespace=namespace,
        MetricData=[
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
    )

def publish_error_metric(namespace, metric_name, trace_id):
    cloudwatch.put_metric_data(
        Namespace=namespace,
        MetricData=[
            {
                "MetricName": metric_name,
                "Value": 1,
                "Unit": "Count",
                "Dimensions": [{"Name": "TraceId", "Value": trace_id}],
            }
        ]
    )
