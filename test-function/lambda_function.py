import os
import json
import boto3
import paramiko
import tempfile
import shutil
import logging
import time
import datetime

from logging_utils import (
    log_job_start, log_job_end, log_sftp_connection, log_matched_files,
    log_checksum_ok, log_checksum_fail, log_file_transferred, log_archive,
    log_tmp_usage, log_warning, log_error
)
from dry_run_utils import is_dry_run_enabled, log_dry_run_action
from checksum_utils import log_checksum
from trace_utils import get_or_create_trace_id
from file_match_utils import match_files
from retry_utils import default_retry
from performance_utils import time_operation
from metrics_utils import publish_file_transfer_metric, publish_error_metric
from alert_utils import send_file_transfer_sns_alert

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')


def get_date_subpath() -> str:
    """Return YYYY/MM/DD string for today's date (local time)."""
    now = datetime.datetime.now()
    return f"{now.year}/{str(now.month).zfill(2)}/{str(now.day).zfill(2)}"


def get_secret(secret_name: str) -> dict:
    """
    Fetch and JSON-decode a secret from AWS Secrets Manager.
    Secret is expected to contain Host / Username / Password fields
    for SFTP endpoints.
    """
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    secret_str = response['SecretString']
    return json.loads(secret_str)


def get_file_patterns():
    """
    Read FILE_PATTERN from env and split on commas.
    Defaults to ['*'] (all files).
    """
    val = os.getenv('FILE_PATTERN')
    if val:
        return [x.strip() for x in val.split(',') if x.strip()]
    return ['*']


@default_retry()
def create_sftp_client(host: str, port: int, username: str, password: str) -> paramiko.SFTPClient:
    """
    Create an SFTP client using username/password auth.
    Wrapped in default_retry() so transient network issues are retried.
    """
    transport = paramiko.Transport((host, port))
    transport.connect(username=username, password=password)
    return paramiko.SFTPClient.from_transport(transport)


@default_retry()
def download_and_upload_to_s3(
    sftp_client: paramiko.SFTPClient,
    remote_dir: str,
    bucket: str,
    prefix: str,
    local_dir: str,
    trace_id: str,
    job_id: str,
    file_patterns,
    metrics: dict,
    transfer_status: dict,
    checksum_status: dict,
    errors: list,
    warnings: list,
):
    """
    Stage 1: Vendor SFTP -> local /tmp -> S3 (dated subfolder).

    - Lists files in remote_dir
    - Filters by file_patterns (FILE_PATTERN)
    - Downloads matching files to local_dir
    - Checksums each file and uploads to S3 under YYYY/MM/DD
    - Populates metrics, transfer_status, checksum_status
    """
    # Normalise remote_dir (no trailing slash)
    remote_dir = remote_dir.rstrip('/') or '.'

    all_files = sftp_client.listdir(remote_dir)
    files = match_files(all_files, include_patterns=file_patterns)
    unmatched = set(all_files) - set(files)

    date_subpath = get_date_subpath()
    log_matched_files(trace_id, files, unmatched)

    total_bytes = 0
    t0 = time.time()

    for filename in files:
        remote_path = f"{remote_dir}/{filename}"
        local_path = os.path.join(local_dir, filename)

        logger.info("[%s] Downloading %s from %s to %s", trace_id, filename, remote_path, local_path)
        # Download from SFTP
        _, duration = time_operation(sftp_client.get, remote_path, local_path)
        bytes_transferred = os.path.getsize(local_path)
        total_bytes += bytes_transferred

        # Checksum before & after (sanity check around S3 upload)
        downloaded_checksum = log_checksum(local_path, trace_id, algo="sha256", note="after SFTP download")
        s3_upload_checksum = log_checksum(local_path, trace_id, algo="sha256", note="before S3 upload")

        if downloaded_checksum == s3_upload_checksum:
            log_checksum_ok(trace_id, filename, downloaded_checksum)
            checksum_status[filename] = f"OK (sha256: {downloaded_checksum})"
        else:
            log_checksum_fail(trace_id, filename, downloaded_checksum, s3_upload_checksum)
            checksum_status[filename] = (
                f"FAIL (downloaded: {downloaded_checksum}, s3: {s3_upload_checksum})"
            )

        s3_key = f"{prefix}/{date_subpath}/{filename}" if prefix else f"{date_subpath}/{filename}"

        if is_dry_run_enabled():
            log_dry_run_action(f"Would upload {filename} to S3 at {s3_key}")
        else:
            logger.info("[%s] Uploading %s to s3://%s/%s", trace_id, filename, bucket, s3_key)
            _, s3_duration = time_operation(s3_client.upload_file, local_path, bucket, s3_key)
            log_file_transferred(trace_id, filename, "S3", s3_duration)
            log_archive(trace_id, filename, s3_key)

    # Aggregate SFTP/S3 throughput metrics
    t1 = time.time()
    download_time = t1 - t0 if t0 else 0.0
    mb = total_bytes / 1024 / 1024 if total_bytes else 0.0
    mbps = (mb / download_time) if download_time else 0.0

    metrics["S3 upload speed mb/s"] = f"{mbps:.2f}"
    metrics["S3 total mb"] = f"{mb:.2f}"
    metrics["SFTP download speed mb/s"] = f"{mbps:.2f}"
    metrics["SFTP total mb"] = f"{mb:.2f}"

    if files:
        transfer_status["s3"] = f"SUCCESS ({', '.join(files)})"
    else:
        msg = "No files matched FILE_PATTERN for SFTP->S3; skipping."
        warnings.append(msg)
        log_warning(trace_id, msg)
        transfer_status["s3"] = "NO FILES"

    # Publish metrics (unless dry run)
    if not is_dry_run_enabled() and files:
        try:
            publish_file_transfer_metric(
                namespace="LambdaFileTransfer",
                direction="SFTP_TO_S3",
                file_count=len(files),
                total_bytes=total_bytes,
                duration_sec=round(download_time, 2),
                trace_id=trace_id,
            )
        except Exception as e:
            log_error(trace_id, "CloudWatch metric error for S3 transfer", exc=e)
            publish_error_metric("LambdaFileTransfer", "S3MetricError", trace_id)
            errors.append(str(e))


@default_retry()
def upload_local_files_to_sftp(
    local_dir: str,
    file_patterns,
    sftp_client: paramiko.SFTPClient,
    remote_dir: str,
    trace_id: str,
    metrics: dict,
    transfer_status: dict,
    errors: list,
    warnings: list,
):
    """
    Stage 2: local /tmp -> Ninepoint SFTP (sftp.ninepoint.com).

    - Lists files in local_dir
    - Filters by file_patterns (same FILE_PATTERN as SFTP->S3)
    - Uploads to Ninepoint SFTP under remote_dir
    - Populates metrics and transfer_status
    """
    remote_dir = remote_dir.rstrip('/') or '/'

    local_files = os.listdir(local_dir)
    files = match_files(local_files, include_patterns=file_patterns)
    unmatched = set(local_files) - set(files)
    log_matched_files(trace_id, files, unmatched)

    if not files:
        msg = "No files matched FILE_PATTERN for Ninepoint SFTP upload; skipping."
        warnings.append(msg)
        log_warning(trace_id, msg)
        transfer_status["ninepoint_sftp"] = "NO FILES"
        return

    total_bytes = 0
    t0 = time.time()

    for filename in files:
        local_path = os.path.join(local_dir, filename)
        remote_path = f"{remote_dir}/{filename}" if remote_dir != "/" else f"/{filename}"

        if is_dry_run_enabled():
            log_dry_run_action(f"Would upload {local_path} to Ninepoint SFTP at {remote_path}")
            continue

        logger.info("[%s] Uploading %s to Ninepoint SFTP at %s", trace_id, filename, remote_path)
        _, duration = time_operation(sftp_client.put, local_path, remote_path)
        size_bytes = os.path.getsize(local_path)
        total_bytes += size_bytes
        mb = size_bytes / 1024 / 1024 if size_bytes else 0.0
        mbps = (mb / duration) if duration else None
        log_file_transferred(trace_id, filename, "Ninepoint SFTP", duration, mbps)

    # Aggregate throughput metrics (only if not dry run and we actually uploaded)
    if not is_dry_run_enabled():
        elapsed = time.time() - t0 if t0 else 0.0
        mb_total = total_bytes / 1024 / 1024 if total_bytes else 0.0
        mbps_total = (mb_total / elapsed) if elapsed else 0.0

        metrics["Ninepoint SFTP upload speed mb/s"] = f"{mbps_total:.2f}"
        metrics["Ninepoint SFTP total mb"] = f"{mb_total:.2f}"
        transfer_status["ninepoint_sftp"] = f"SUCCESS ({', '.join(files)})"

        try:
            publish_file_transfer_metric(
                namespace="LambdaFileTransfer",
                direction="S3_TO_SFTP",
                file_count=len(files),
                total_bytes=total_bytes,
                duration_sec=round(elapsed, 2),
                trace_id=trace_id,
            )
        except Exception as e:
            log_error(trace_id, "CloudWatch metric error for Ninepoint SFTP transfer", exc=e)
            publish_error_metric("LambdaFileTransfer", "NinepointSFTPMetricError", trace_id)
            errors.append(str(e))


def lambda_handler(event, context):
    """
    Salesforce / Unitrax pipeline Lambda:

    1) Connect to vendor SFTP (CRMEXTHLD / CRMEXTNAV / CRMEXTTRN / CRMEXTREP)
       using SRC_SECRET_NAME and SRC_REMOTE_DIR.
    2) Download matching files (FILE_PATTERN) to /tmp and upload to S3
       under S3_BUCKET/S3_PREFIX/YYYY/MM/DD.
    3) Upload the same files from /tmp to Ninepoint SFTP
       (DEST_SECRET_NAME, DEST_REMOTE_DIR).
    4) Send SNS summary including transfer_status, checksum_status, and metrics.

    Box is intentionally NOT used in this function.
    """
    trace_id = get_or_create_trace_id(context)
    job_id = trace_id
    file_patterns = get_file_patterns()

    log_job_start(trace_id, job_id, file_patterns)

    # Env configuration
    src_secret_name = os.getenv("SRC_SECRET_NAME")
    dest_secret_name = os.getenv("DEST_SECRET_NAME")

    s3_bucket = os.getenv("S3_BUCKET", "jams-ftp-process-bucket")
    s3_prefix = os.getenv("S3_PREFIX", "salesforce")
    sns_topic_arn = os.getenv("SNS_TOPIC_ARN")

    src_remote_dir = os.getenv("SRC_REMOTE_DIR", ".")
    dest_remote_dir = os.getenv("DEST_REMOTE_DIR", "/npunitrax/")

    # Secrets for SFTP endpoints
    src_secret = get_secret(src_secret_name)
    src_host = src_secret["Host"]
    src_user = src_secret["Username"]
    src_pass = src_secret["Password"]

    dest_secret = get_secret(dest_secret_name)
    dest_host = dest_secret["Host"]
    dest_user = dest_secret["Username"]
    dest_pass = dest_secret["Password"]

    metrics: dict = {}
    transfer_status: dict = {}
    checksum_status: dict = {}
    errors: list = []
    warnings: list = []

    with tempfile.TemporaryDirectory() as tmp_dir:
        # Initial temp usage
        free_mb = shutil.disk_usage(tmp_dir).free // (1024 * 1024)
        log_tmp_usage(trace_id, len(os.listdir(tmp_dir)), free_mb)

        # --- Stage 1: Vendor SFTP -> local /tmp -> S3 ---
        src_sftp = create_sftp_client(src_host, 22, src_user, src_pass)
        log_sftp_connection(trace_id, src_host, "OPENED")

        download_and_upload_to_s3(
            src_sftp,
            src_remote_dir,
            s3_bucket,
            s3_prefix,
            tmp_dir,
            trace_id,
            job_id,
            file_patterns,
            metrics,
            transfer_status,
            checksum_status,
            errors,
            warnings,
        )

        src_sftp.close()
        log_sftp_connection(trace_id, src_host, "CLOSED")

        free_mb = shutil.disk_usage(tmp_dir).free // (1024 * 1024)
        log_tmp_usage(trace_id, len(os.listdir(tmp_dir)), free_mb)

        # --- Stage 2: local /tmp -> Ninepoint SFTP ---
        dest_sftp = create_sftp_client(dest_host, 22, dest_user, dest_pass)
        log_sftp_connection(trace_id, dest_host, "OPENED")

        upload_local_files_to_sftp(
            tmp_dir,
            file_patterns,
            dest_sftp,
            dest_remote_dir,
            trace_id,
            metrics,
            transfer_status,
            errors,
            warnings,
        )

        dest_sftp.close()
        log_sftp_connection(trace_id, dest_host, "CLOSED")

        free_mb = shutil.disk_usage(tmp_dir).free // (1024 * 1024)
        log_tmp_usage(trace_id, len(os.listdir(tmp_dir)), free_mb)

    # Send SNS Alert (always runs, even for dry run)
    if sns_topic_arn:
        send_file_transfer_sns_alert(
            sns_topic_arn,
            trace_id,
            transfer_status=transfer_status,
            checksum_status=checksum_status,
            errors=errors,
            warnings=warnings,
            function_name=(context.function_name if context else "lambda_handler"),
        )
    else:
        log_warning(trace_id, "SNS_TOPIC_ARN not set; skipping SNS alert.")

    log_job_end(trace_id, job_id)

    body = {
        "message": "Salesforce files transferred via Lambda.",
        "trace_id": trace_id,
        "transfer_status": transfer_status,
        "checksum_status": checksum_status,
        "errors": errors,
        "warnings": warnings,
        "metrics": metrics,
    }

    return {
        "statusCode": 200,
        "body": json.dumps(body),
    }
