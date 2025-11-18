import os
import boto3
import logging

logger = logging.getLogger(__name__)

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
    sns_topic_arn = os.getenv("SNS_TOPIC_ARN")
    if not sns_topic_arn:
        logger.warning("SNS_TOPIC_ARN is not set. No alert sent.")
        return

    s3_display  = ", ".join(s3_files)  if s3_files  else "NO FILES"
    box_display = ", ".join(box_files) if box_files else "NO FILES"

    if ftp_host:
        files_display = ", ".join(ftp_files) if ftp_files else "NO FILES"
        ftp_line = f"Transferred to FTP ({ftp_host}): {files_display}"
    else:
        ftp_line = f"Transferred to FTP: " + (", ".join(ftp_files) if ftp_files else "NO FILES")

    body = (
        f"[ALERT] File Transfer Summary\n"
        f"Function: {function_name}\n"
        f"Trace ID: {trace_id}\n\n"
        f"Transferred to S3: {s3_display}\n"
        f"Transferred to Box: {box_display}\n"
        f"{ftp_line}\n\n"
    )

    if warnings:
        body += "Warnings:\n" + "\n".join([str(w) for w in warnings]) + "\n"
    if errors:
        body += "Errors:\n" + "\n".join([str(e) for e in errors]) + "\n"

    sev = "ERROR" if errors else ("WARN" if warnings else "INFO")
    subject = f"[{sev}] File Transfer Alert (Trace ID: {trace_id})"

    resp = boto3.client("sns").publish(
        TopicArn=sns_topic_arn,
        Subject=subject,
        Message=body,
        MessageAttributes={
            "Severity":   {"DataType": "String", "StringValue": sev},
            "Function":   {"DataType": "String", "StringValue": function_name or "N/A"},
            "TraceId":    {"DataType": "String", "StringValue": trace_id or "N/A"},
            "PipelineOk": {"DataType": "String", "StringValue": "false" if (errors or warnings) else "true"},
        },
    )
    logger.info(f"Sent SNS alert to {sns_topic_arn} (MessageId={resp.get('MessageId')})")
