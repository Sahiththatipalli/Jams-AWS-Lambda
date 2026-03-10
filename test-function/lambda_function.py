import os
import json
import boto3
import paramiko
import tempfile
import shutil
import logging
import time

from logging_utils import (
    log_job_start,
    log_job_end,
    log_sftp_connection,
    log_matched_files,
    log_error,
)
from dry_run_utils import is_dry_run_enabled, log_dry_run_action
from file_match_utils import match_files
from storage_utils import get_date_subpath

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")
secrets_client = boto3.client("secretsmanager")
lambda_client = boto3.client("lambda")
sns_client = boto3.client("sns")


def get_trace_id(context) -> str:
    """Simple trace identifier based on the Lambda request id."""
    return getattr(context, "aws_request_id", "no-trace-id")


def get_sftp_credentials(secret_name: str) -> dict:
    """Retrieve SFTP credentials JSON from Secrets Manager."""
    resp = secrets_client.get_secret_value(SecretId=secret_name)
    secret_str = resp.get("SecretString") or ""
    data = json.loads(secret_str)
    return {
        "host": data.get("Host") or data.get("host"),
        "username": data.get("Username") or data.get("username"),
        "password": data.get("Password") or data.get("password"),
        "port": int(data.get("Port") or data.get("port") or 22),
    }


def create_sftp_client(creds: dict) -> paramiko.SFTPClient:
    """Create an SFTP client using host/port/username/password."""
    transport = paramiko.Transport((creds["host"], creds["port"]))
    transport.connect(username=creds["username"], password=creds["password"])
    return paramiko.SFTPClient.from_transport(transport)


def list_matching_files(sftp: paramiko.SFTPClient, directory: str, patterns):
    """Return list of filenames in directory that match any pattern."""
    directory = directory.rstrip("/") or "."
    all_files = sftp.listdir(directory)
    matched = match_files(all_files, include_patterns=patterns)
    unmatched = sorted(set(all_files) - set(matched))
    return directory, matched, unmatched


def move_and_archive_files(
    trace_id: str,
    sftp: paramiko.SFTPClient,
    src_dir: str,
    dest_dir: str,
    bucket: str,
    s3_prefix: str,
    file_patterns,
    delete_source: bool = True,
):
    """Core logic: src_dir -> dest_dir (same SFTP) and archive to S3.

    Returns (moved_files, errors)
    """
    src_dir, files, unmatched = list_matching_files(sftp, src_dir, file_patterns)
    log_matched_files(trace_id, files, unmatched)

    if not files:
        logger.warning(
            "[%s] No files matched patterns %s in %s",
            trace_id, file_patterns, src_dir
        )
        return [], []

    dest_dir = dest_dir.rstrip("/") or "/"

    moved_files = []
    errors = []
    date_subpath = get_date_subpath()

    total_bytes = 0
    t_start = time.time()

    for filename in files:
        src_path = f"{src_dir}/{filename}"
        dest_path = f"{dest_dir}/{filename}"
        local_path = os.path.join(tempfile.gettempdir(), filename)

        if is_dry_run_enabled():
            log_dry_run_action(
                f"Would move {src_path} -> {dest_path} on same SFTP and archive "
                f"to s3://{bucket}/{s3_prefix}/{date_subpath}/{filename}"
            )
            moved_files.append(filename)
            continue

        try:
            # Download from source dir
            logger.info("[%s] Downloading %s to %s", trace_id, src_path, local_path)
            sftp.get(src_path, local_path)
            size_bytes = os.path.getsize(local_path)
            total_bytes += size_bytes

            # Ensure destination dir exists (best effort)
            try:
                sftp.chdir(dest_dir)
            except IOError:
                parts = [p for p in dest_dir.split("/") if p]
                path = ""
                for part in parts:
                    path += "/" + part
                    try:
                        sftp.mkdir(path)
                    except IOError:
                        pass

            # Upload to destination dir
            logger.info("[%s] Uploading %s to %s", trace_id, local_path, dest_path)
            sftp.put(local_path, dest_path)

            # Archive to S3
            s3_key = (
                f"{s3_prefix}/{date_subpath}/{filename}"
                if s3_prefix
                else f"{date_subpath}/{filename}"
            )
            logger.info(
                "[%s] Uploading %s to s3://%s/%s",
                trace_id, local_path, bucket, s3_key
            )
            s3_client.upload_file(local_path, bucket, s3_key)

            # Delete from source dir if requested
            if delete_source:
                logger.info("[%s] Deleting source %s", trace_id, src_path)
                sftp.remove(src_path)

            moved_files.append(filename)

        except Exception as exc:
            log_error(trace_id, f"Failed processing {src_path}", exc=exc)
            errors.append(f"{filename}: {exc}")

        finally:
            try:
                if os.path.exists(local_path):
                    os.remove(local_path)
            except Exception:
                pass

    elapsed = time.time() - t_start
    if not is_dry_run_enabled() and moved_files and elapsed > 0:
        mb = total_bytes / 1024 / 1024
        logger.info(
            "[%s] Moved %d file(s), %.2f MB total in %.2f s (%.2f MB/s)",
            trace_id,
            len(moved_files),
            mb,
            elapsed,
            mb / elapsed if elapsed else 0.0,
        )

    return moved_files, errors


def trigger_salesforce_dataloader(trace_id: str, moved_files, extra_payload=None):
    """Invoke the Salesforce Dataloader Lambda asynchronously after move."""
    # You can set this explicitly in env; otherwise it defaults to literal name.
    fn_name = os.getenv("SALESFORCE_DATALOADER_FUNCTION", "Salesforce Dataloader")

    payload = {
        "source": "salesforce-sftp-move",
        "trace_id": trace_id,
        "moved_files": moved_files,
    }
    if extra_payload:
        payload.update(extra_payload)

    if is_dry_run_enabled():
        log_dry_run_action(
            f"Would invoke Lambda '{fn_name}' with payload: {json.dumps(payload)}"
        )
        return

    try:
        lambda_client.invoke(
            FunctionName=fn_name,
            InvocationType="Event",  # async / fire-and-forget
            Payload=json.dumps(payload).encode("utf-8"),
        )
        logger.info(
            "[%s] Triggered Salesforce Dataloader Lambda '%s'",
            trace_id,
            fn_name,
        )
    except Exception as exc:
        log_error(
            trace_id,
            f"Failed to invoke Salesforce Dataloader Lambda '{fn_name}'",
            exc=exc,
        )


def publish_sns_summary(
    trace_id: str,
    sns_topic_arn: str,
    moved_files,
    errors,
    src_dir,
    dest_dir,
    bucket,
    s3_prefix,
):
    if not sns_topic_arn:
        return

    subject = f"Salesforce SFTP move summary [{trace_id}]"
    body = {
        "trace_id": trace_id,
        "src_dir": src_dir,
        "dest_dir": dest_dir,
        "s3_bucket": bucket,
        "s3_prefix": s3_prefix,
        "moved_files": moved_files,
        "error_count": len(errors),
        "errors": errors,
    }

    if is_dry_run_enabled():
        log_dry_run_action(
            f"Would publish SNS summary to {sns_topic_arn}: {json.dumps(body)}"
        )
        return

    sns_client.publish(
        TopicArn=sns_topic_arn,
        Subject=subject,
        Message=json.dumps(body, indent=2),
    )
    logger.info("[%s] Published SNS summary to %s", trace_id, sns_topic_arn)


def _get_file_patterns():
    raw = os.getenv("FILE_PATTERN")
    if not raw:
        return ["*"]
    return [p.strip() for p in raw.split(",") if p.strip()]


def lambda_handler(event, context):
    trace_id = get_trace_id(context)
    job_id = trace_id

    # Env configuration
    secret_name = os.environ["SFTP_SECRET_NAME"]
    src_remote_dir = os.getenv("SRC_REMOTE_DIR", "/lti/prod")
    dest_remote_dir = os.getenv("DEST_REMOTE_DIR", "/npunitrax")
    file_patterns = _get_file_patterns()
    s3_bucket = os.environ["S3_BUCKET"]
    s3_prefix = os.getenv("S3_PREFIX", "salesforce")
    delete_source = str(os.getenv("DELETE_SOURCE", "true")).lower() == "true"
    sns_topic_arn = os.getenv("SNS_TOPIC_ARN")

    log_job_start(trace_id, job_id, file_patterns)

    creds = get_sftp_credentials(secret_name)
    sftp = create_sftp_client(creds)
    log_sftp_connection(trace_id, creds["host"], "OPENED")

    try:
        moved_files, errors = move_and_archive_files(
            trace_id,
            sftp,
            src_remote_dir,
            dest_remote_dir,
            s3_bucket,
            s3_prefix,
            file_patterns,
            delete_source=delete_source,
        )
    finally:
        sftp.close()
        log_sftp_connection(trace_id, creds["host"], "CLOSED")

    # SNS summary
    publish_sns_summary(
        trace_id,
        sns_topic_arn,
        moved_files,
        errors,
        src_remote_dir,
        dest_remote_dir,
        s3_bucket,
        s3_prefix,
    )

    # 🔹 Trigger Salesforce Dataloader only if files were actually moved
    if moved_files:
        trigger_salesforce_dataloader(
            trace_id,
            moved_files,
            extra_payload={
                "src_dir": src_remote_dir,
                "dest_dir": dest_remote_dir,
                "s3_bucket": s3_bucket,
                "s3_prefix": s3_prefix,
            },
        )
    else:
        logger.info("[%s] No files moved; not triggering Salesforce Dataloader.", trace_id)

    log_job_end(trace_id, job_id)

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "Completed SFTP move and archive.",
                "trace_id": trace_id,
                "moved_files": moved_files,
                "error_count": len(errors),
            }
        ),
    }
