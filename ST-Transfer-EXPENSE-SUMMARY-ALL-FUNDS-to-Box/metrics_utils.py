import boto3

def publish_file_transfer_metric(namespace, direction, file_count, total_bytes, duration_sec, trace_id):
    cloudwatch = boto3.client('cloudwatch')
    cloudwatch.put_metric_data(
        Namespace=namespace,
        MetricData=[
            {'MetricName': 'FileTransferCount', 'Value': file_count, 'Unit': 'Count', 'Dimensions': [{'Name': 'Direction', 'Value': direction}, {'Name': 'TraceId', 'Value': trace_id}]},
            {'MetricName': 'FileTransferBytes', 'Value': total_bytes, 'Unit': 'Bytes', 'Dimensions': [{'Name': 'Direction', 'Value': direction}, {'Name': 'TraceId', 'Value': trace_id}]},
            {'MetricName': 'FileTransferDuration', 'Value': duration_sec, 'Unit': 'Seconds', 'Dimensions': [{'Name': 'Direction', 'Value': direction}, {'Name': 'TraceId', 'Value': trace_id}]},
        ]
    )

def publish_error_metric(namespace, error_type, trace_id):
    cloudwatch = boto3.client('cloudwatch')
    cloudwatch.put_metric_data(
        Namespace=namespace,
        MetricData=[
            {'MetricName': 'Error', 'Value': 1, 'Unit': 'Count', 'Dimensions': [{'Name': 'ErrorType', 'Value': error_type}, {'Name': 'TraceId', 'Value': trace_id}]}
        ]
    )
