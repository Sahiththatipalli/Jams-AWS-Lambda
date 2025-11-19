import boto3
import os

sns_client = boto3.client('sns')

def send_file_transfer_sns_alert(trace_id, s3_files, box_files, checksum_results, errors=None, warnings=None, function_name="N/A", dry_run_enabled=False, transfer_status="UNKNOWN"):
    """
    Send SNS alert with summary of transfer only if errors or warnings exist.
    """

    # Do not send alert if no errors
    if not errors and not warnings:
        return

    sns_topic_arn = os.getenv("SNS_TOPIC_ARN", "")

    body = f"""[ALERT] File Transfer Summary
Function: {function_name}
Trace ID: {trace_id}

TRANSFER STATUS: {transfer_status}

Transferred to S3: {', '.join(s3_files) if s3_files else 'None'}
Transferred to Box: {', '.join(box_files) if box_files else 'None'}
"""

    if warnings:
        body += "\nWarnings:\n" + "\n".join([str(w) for w in warnings])
    if errors:
        body += "\nErrors:\n" + "\n".join([str(e) for e in errors])

    sns_client.publish(
        TopicArn=sns_topic_arn,
        Subject=f"File Transfer Alert (Trace ID: {trace_id})",
        Message=body
    )
