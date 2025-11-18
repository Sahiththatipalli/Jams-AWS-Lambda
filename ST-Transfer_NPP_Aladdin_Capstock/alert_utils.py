import os
import boto3

def send_file_transfer_sns_alert(
    trace_id,
    s3_files,
    box_files,
    ftp_files,
    ftp_host=None,
    errors=None,
    warnings=None,
    function_name="N/A"
):
    sns_client = boto3.client("sns")
    sns_topic_arn = os.getenv("SNS_TOPIC_ARN")

    s3_display = ', '.join(s3_files) if s3_files else "NO FILES"
    box_display = ', '.join(box_files) if box_files else "NO FILES"

    if ftp_host:
        files_display = ', '.join(ftp_files) if ftp_files else "NO FILES"
        ftp_display = f"{files_display}"
        ftp_line = f"Transferred to FTP ({ftp_host}): {ftp_display}"
    else:
        ftp_line = f"Transferred to FTP: {', '.join(ftp_files) if ftp_files else 'NO FILES'}"

    body = f"""[ALERT] File Transfer Summary
Function: {function_name}
Trace ID: {trace_id}

Transferred to S3: {s3_display}
Transferred to Box: {box_display}
{ftp_line}

"""

    # Checksums section removed as requested

    if warnings:
        body += "Warnings:\n" + "\n".join([str(w) for w in warnings]) + "\n"
    if errors:
        body += "Errors:\n" + "\n".join([str(e) for e in errors]) + "\n"

    if sns_topic_arn:
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Subject=f"File Transfer Alert (Trace ID: {trace_id})",
            Message=body
        )
    else:
        print("[WARNING] SNS_TOPIC_ARN is not set. No alert sent.")
