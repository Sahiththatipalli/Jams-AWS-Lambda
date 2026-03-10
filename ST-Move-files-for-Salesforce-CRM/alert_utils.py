import boto3

sns_client = boto3.client("sns")

def send_file_transfer_sns_alert(
    sns_topic_arn, trace_id,
    transfer_status, checksum_status,
    errors=None, warnings=None, function_name="lambda_function"
):
    """Send an SNS alert confirming transfer and checksums."""
    body = f"""AWS Lambda File Transfer Alert
Function Name: {function_name}
Trace ID: {trace_id}

==== Transfer Status ====
- S3: {transfer_status.get('s3', 'N/A')}
- BOX: {transfer_status.get('box', 'N/A')}

==== Checksum Results ====
"""
    for f, status in checksum_status.items():
        body += f"- {f}: {status}\n"
    if warnings:
        body += "\n==== Warnings ====\n" + "\n".join([str(w) for w in warnings])
    if errors:
        body += "\n==== Errors ====\n" + "\n".join([str(e) for e in errors])

    body += "\n\nAutomated Lambda SNS Alert. See CloudWatch for full logs."

    sns_client.publish(
        TopicArn=sns_topic_arn,
        Subject=f"Lambda File Transfer Alert (Trace ID: {trace_id})",
        Message=body
    )
