import boto3
import os

SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN")
sns_client = boto3.client("sns")

def send_file_transfer_sns_alert(
    trace_id,
    s3_files,
    ftp_files,
    checksum_results,
    errors=None,
    warnings=None,
    function_name="N/A",
    ftp_server_name="N/A"
):
    """
    Sends an SNS alert including only errors, warnings, and checksum failures.
    Skips sending if no issues are found.
    """
    errors = errors or []
    warnings = warnings or []

    failed_checksums = [c for c in checksum_results if "FAIL" in c['status']]

    # Skip sending if no issues
    if not errors and not warnings and not failed_checksums:
        return

    body = f"""AWS Lambda File Transfer Alert
Function: {function_name}
FTP Server: {ftp_server_name}
Trace ID: {trace_id}

"""

    if errors:
        body += "==== Errors ====\n" + "\n".join([str(e) for e in errors]) + "\n\n"

    if warnings:
        body += "==== Warnings ====\n" + "\n".join([str(w) for w in warnings]) + "\n\n"

    if failed_checksums:
        body += "==== Checksum Failures ====\n"
        body += "\n".join([f"- {c['file']}: {c['status']}" for c in failed_checksums]) + "\n\n"

    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f"File Transfer Alert (Trace ID: {trace_id})",
        Message=body
    )
