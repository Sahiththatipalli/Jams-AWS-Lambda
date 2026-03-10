import os
import json
import time
import logging
import datetime
import tempfile
import shutil

import boto3
import paramiko

from logging_utils import (
    log_job_start,
    log_job_end,
    log_sftp_connection,
    log_matched_files,
    log_file_transferred,
    log_tmp_usage,
    log_warning,
    log_error,
)
from dry_run_utils import is_dry_run_enabled, log_dry_run_action
from file_match_utils import match_files
from storage_utils import get_date_subpath
from trace_utils import get_or_create_trace_id
from retry_utils import default_retry
from performance_utils import time_operation
from metrics_utils import publish_file_transfer_metric, publish_error_metric
from alert_utils import send_file_transfer_sns_alert

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")
secrets_client = boto3.client("secretsmanager")
lambda_client = boto3.client("lambda")


# ---------------------------------------------------------------------------
# Helpers – dates, patterns, SFTP creds
# ---------------------------------------------------------------------------

def previous_business_day(ref_date: datetime.date | None = None) -> datetime.date:
    """
    Return previous business day (Mon–Fri, no holiday logic).
    Used for Import_Date__c passed to the Salesforce Dataloader Lambda.
    """
    if ref_date is None:
        ref_date = datetime.date.today()
    d = ref_date - datetime.timedelta(days=1)
    while d.weekday() >= 5:  # 5=Sat, 6=Sun
        d -= datetime.timedelta(days=1)
    return d


def _get_file_patterns():
    """
    FILE_PATTERN can be something like:
       "CRMEXTHLD.*,CRMEXTNAV.*,CRMEXTTRN.*"
    If not set, we default to those three.
    """
    raw = os.getenv("FILE_PATTERN")
    if not raw:
        return ["CRMEXTHLD.*", "CRMEXTNAV.*", "CRMEXTTRN.*"]
    return [p.strip() for p in raw.split(",") if p.strip()]


def get_sftp_credentials(secret_name: str) -> dict:
    """
    Retrieve SFTP credentials JSON from Secrets Manager.

    Expected JSON shape:
    {
      "Host": "sftp.ninepoint.com",
      "Username": "appservice",
      "Password": "********",
      "Port": 22
    }
    """
    resp = secrets_client.get_secret_value(SecretId=secret_name)
    secret_str = resp.get("SecretString") or ""
    data = json.loads(secret_str)

    host = data.get("Host") or data.get("host")
    username = data.get("Username") or data.get("username")
    password = data.get("Password") or data.get("password")
    port = int(data.get("Port") or data.get("port") or 22)

    if not all([host, username, password]):
        raise RuntimeError("SFTP secret must contain Host, Username and Password fields.")

    return {
        "host": host,
        "username": username,
        "password": password,
        "port": port,
    }


@default_retry()
def create_sftp_client(creds: dict) -> paramiko.SFTPClient:
    """
    Create an SFTP client using host/port/username/password.
    """
    transport = paramiko.Transport((creds["host"], creds["port"]))
    transport.connect(username=creds["username"], password=creds["password"])
    return paramiko.SFTPClient.from_transport(transport)


def infer_missing_flags_from_moved_files(moved_files: list[str]) -> dict:
    """
    From the list of files we actually moved, infer which of HLD/NAV/TRN are missing.

    Logic: check for substrings in the filenames:
      - HLD  -> "CRMEXTHLD"
      - NAV  -> "CRMEXTNAV"
      - TRN  -> "CRMEXTTRN"
    """
    present_types = set()
    for fname in moved_files:
        up = str(fname).upper()
        if "CRMEXTHLD" in up:
            present_types.add("HLD")
        if "CRMEXTNAV" in up:
            present_types.add("NAV")
        if "CRMEXTTRN" in up:
            present_types.add("TRN")

    all_types = ["HLD", "NAV", "TRN"]
    missing_flags = {t: (t not in present_types) for t in all_types}
    return missing_flags


# ---------------------------------------------------------------------------
# Core file processing: /lti/prod -> /npunitrax + S3
# ---------------------------------------------------------------------------

def process_files_on_sftp(
    trace_id: str,
    sftp: paramiko.SFTPClient,
    src_dir: str,
    dest_dir: str,
    bucket: str,
    s3_prefix: str,
    file_patterns: list[str],
    delete_source: bool,
    date_subpath: str,
    transfer_status: dict,
    metrics: dict,
    errors: list,
    warnings: list,
) -> list[str]:
    """
    Main worker:

    1. List files in src_dir on the SFTP server.
    2. Filter using FILE_PATTERN.
    3. For each matched file:
       - download from src_dir to /tmp
       - upload to dest_dir on the SAME SFTP server
       - upload to S3 under s3://bucket/s3_prefix/date_subpath/filename
       - optionally delete from src_dir once both uploads succeed.

    Returns list of filenames that were successfully moved.
    """
    src_dir = src_dir.rstrip("/") or "."
    dest_dir = dest_dir.rstrip("/") or "/"

    all_files = sftp.listdir(src_dir)
    files = match_files(all_files, include_patterns=file_patterns)
    unmatched = sorted(set(all_files) - set(files))
    log_matched_files(trace_id, files, unmatched)

    if not files:
        msg = f"No files in {src_dir} matched patterns {file_patterns}; nothing to do."
        log_warning(trace_id, msg)
        transfer_status["sftp_move"] = "NO FILES"
        transfer_status["s3"] = "NO FILES"
        warnings.append(msg)
        return []

    moved_files: list[str] = []

    total_bytes_download = 0
    total_bytes_upload_sftp = 0
    total_bytes_upload_s3 = 0

    total_time_download = 0.0
    total_time_upload_sftp = 0.0
    total_time_upload_s3 = 0.0

    for filename in files:
        src_path = f"{src_dir}/{filename}"
        dest_path = f"{dest_dir}/{filename}"
        local_path = os.path.join(tempfile.gettempdir(), filename)

        if is_dry_run_enabled():
            log_dry_run_action(
                f"Would download {src_path} to {local_path}, "
                f"upload to {dest_path} on same SFTP, and "
                f"archive to s3://{bucket}/{s3_prefix}/{date_subpath}/{filename}"
            )
            moved_files.append(filename)
            continue

        try:
            # --- Download from source folder ---
            logger.info("[%s] Downloading %s to %s", trace_id, src_path, local_path)
            _, dl_time = time_operation(sftp.get, src_path, local_path)
            size_bytes = os.path.getsize(local_path)
            total_bytes_download += size_bytes
            total_time_download += dl_time

            # --- Ensure destination dir exists (best-effort) ---
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

            # --- Upload to destination dir (same SFTP server) ---
            logger.info("[%s] Uploading %s to %s", trace_id, local_path, dest_path)
            _, up_time_sftp = time_operation(sftp.put, local_path, dest_path)
            total_bytes_upload_sftp += size_bytes
            total_time_upload_sftp += up_time_sftp

            mb = size_bytes / 1024 / 1024 if size_bytes else 0.0
            mbps_sftp = (mb / up_time_sftp) if up_time_sftp else None
            log_file_transferred(trace_id, filename, "SFTP_MOVE", up_time_sftp, mbps_sftp)

            # --- Upload to S3 archive ---
            s3_key = (
                f"{s3_prefix}/{date_subpath}/{filename}"
                if s3_prefix
                else f"{date_subpath}/{filename}"
            )
            logger.info(
                "[%s] Uploading %s to s3://%s/%s",
                trace_id,
                local_path,
                bucket,
                s3_key,
            )
            _, up_time_s3 = time_operation(
                s3_client.upload_file, local_path, bucket, s3_key
            )
            total_bytes_upload_s3 += size_bytes
            total_time_upload_s3 += up_time_s3

            mbps_s3 = (mb / up_time_s3) if up_time_s3 else None
            log_file_transferred(trace_id, filename, "S3", up_time_s3, mbps_s3)

            # --- Delete from source directory after successful move + S3 (if configured) ---
            if delete_source:
                try:
                    logger.info("[%s] Deleting source file %s", trace_id, src_path)
                    sftp.remove(src_path)
                except Exception as e_del:
                    msg = f"Failed to delete source file {src_path}"
                    log_error(trace_id, msg, exc=e_del)
                    warnings.append(f"{msg}: {e_del}")

            moved_files.append(filename)

        except Exception as e:
            msg = f"Failed processing {src_path}"
            log_error(trace_id, msg, exc=e)
            errors.append(f"{msg}: {e}")

        finally:
            try:
                if os.path.exists(local_path):
                    os.remove(local_path)
            except Exception:
                pass

    def _calc_speed(total_bytes, total_time):
        if not total_bytes or not total_time:
            return 0.0, 0.0
        mb_ = total_bytes / 1024 / 1024
        mbps_ = mb_ / total_time if total_time else 0.0
        return mb_, mbps_

    mb_dl, mbps_dl = _calc_speed(total_bytes_download, total_time_download)
    mb_up_sftp, mbps_up_sftp = _calc_speed(total_bytes_upload_sftp, total_time_upload_sftp)
    mb_up_s3, mbps_up_s3 = _calc_speed(total_bytes_upload_s3, total_time_upload_s3)

    metrics["SFTP download total mb"] = f"{mb_dl:.2f}"
    metrics["SFTP download speed mb/s"] = f"{mbps_dl:.2f}"
    metrics["SFTP move total mb"] = f"{mb_up_sftp:.2f}"
    metrics["SFTP move speed mb/s"] = f"{mbps_up_sftp:.2f}"
    metrics["S3 upload total mb"] = f"{mb_up_s3:.2f}"
    metrics["S3 upload speed mb/s"] = f"{mbps_up_s3:.2f}"

    if not is_dry_run_enabled():
        try:
            if total_bytes_download:
                publish_file_transfer_metric(
                    namespace="LambdaFileTransfer",
                    direction="SFTP_TO_TMP",
                    file_count=len(moved_files),
                    total_bytes=total_bytes_download,
                    duration_sec=round(total_time_download, 2),
                    trace_id=trace_id,
                )
        except Exception as e:
            log_error(trace_id, "CloudWatch metric error for SFTP download", exc=e)
            publish_error_metric("LambdaFileTransfer", "SFTPDownloadMetricError", trace_id)

        try:
            if total_bytes_upload_sftp:
                publish_file_transfer_metric(
                    namespace="LambdaFileTransfer",
                    direction="TMP_TO_SFTP",
                    file_count=len(moved_files),
                    total_bytes=total_bytes_upload_sftp,
                    duration_sec=round(total_time_upload_sftp, 2),
                    trace_id=trace_id,
                )
        except Exception as e:
            log_error(trace_id, "CloudWatch metric error for SFTP move", exc=e)
            publish_error_metric("LambdaFileTransfer", "SFTPMoveMetricError", trace_id)

        try:
            if total_bytes_upload_s3:
                publish_file_transfer_metric(
                    namespace="LambdaFileTransfer",
                    direction="TMP_TO_S3",
                    file_count=len(moved_files),
                    total_bytes=total_bytes_upload_s3,
                    duration_sec=round(total_time_upload_s3, 2),
                    trace_id=trace_id,
                )
        except Exception as e:
            log_error(trace_id, "CloudWatch metric error for S3 upload", exc=e)
            publish_error_metric("LambdaFileTransfer", "S3UploadMetricError", trace_id)

    if is_dry_run_enabled():
        transfer_status["sftp_move"] = f"DRY_RUN ({', '.join(files)})"
        transfer_status["s3"] = f"DRY_RUN ({', '.join(files)})"
    else:
        transfer_status["sftp_move"] = f"SUCCESS ({', '.join(moved_files)})"
        transfer_status["s3"] = f"SUCCESS ({', '.join(moved_files)})"

    return moved_files


# ---------------------------------------------------------------------------
# Salesforce Dataloader trigger
# ---------------------------------------------------------------------------

def trigger_salesforce_dataloader(trace_id: str, moved_files: list[str], extra_payload: dict | None = None):
    """
    Invoke the Salesforce Dataloader Lambda asynchronously (Event invocation).
    """
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
            InvocationType="Event",
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


# ---------------------------------------------------------------------------
# Lambda handler
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    """
    Salesforce SFTP mover:

    - Connects to sftp.ninepoint.com (or configured host).
    - Reads files in SRC_REMOTE_DIR (e.g. /lti/prod) matching FILE_PATTERN.
    - Downloads them to /tmp.
    - Uploads the same files into DEST_REMOTE_DIR (e.g. /npunitrax) on the SAME SFTP server.
    - Uploads the files to S3 under s3://S3_BUCKET/S3_PREFIX/YYYY/MM/DD/filename.
    - Optionally deletes them from SRC_REMOTE_DIR (DELETE_SOURCE=true|false).
    - Sends an SNS summary.
    - Triggers the Salesforce Dataloader Lambda with:
        moved_files, s3_bucket, s3_prefix, date_subpath, missing_files, import_date.
    """
    trace_id = get_or_create_trace_id(context)
    job_id = getattr(context, "aws_request_id", "N/A")

    file_patterns = _get_file_patterns()

    secret_name = os.environ["SFTP_SECRET_NAME"]
    src_remote_dir = os.getenv("SRC_REMOTE_DIR", "/lti/prod")
    dest_remote_dir = os.getenv("DEST_REMOTE_DIR", "/npunitrax")

    s3_bucket = os.environ["S3_BUCKET"]
    s3_prefix = os.getenv("S3_PREFIX", "salesforce/raw")

    delete_source = str(os.getenv("DELETE_SOURCE", "true")).lower() == "true"
    sns_topic_arn = os.getenv("SNS_TOPIC_ARN")

    date_subpath = get_date_subpath()
    import_date = previous_business_day().isoformat()

    log_job_start(trace_id, job_id, file_patterns)

    creds = get_sftp_credentials(secret_name)
    sftp = create_sftp_client(creds)
    log_sftp_connection(trace_id, creds["host"], "OPENED")

    metrics: dict = {}
    transfer_status: dict = {}
    checksum_status: dict = {}
    errors: list = []
    warnings: list = []

    with tempfile.TemporaryDirectory() as tmp_dir:
        free_mb = shutil.disk_usage(tmp_dir).free // (1024 * 1024)
        log_tmp_usage(trace_id, len(os.listdir(tmp_dir)), free_mb)

        try:
            moved_files = process_files_on_sftp(
                trace_id=trace_id,
                sftp=sftp,
                src_dir=src_remote_dir,
                dest_dir=dest_remote_dir,
                bucket=s3_bucket,
                s3_prefix=s3_prefix,
                file_patterns=file_patterns,
                delete_source=delete_source,
                date_subpath=date_subpath,
                transfer_status=transfer_status,
                metrics=metrics,
                errors=errors,
                warnings=warnings,
            )
        finally:
            sftp.close()
            log_sftp_connection(trace_id, creds["host"], "CLOSED")

        free_mb = shutil.disk_usage(tmp_dir).free // (1024 * 1024)
        log_tmp_usage(trace_id, len(os.listdir(tmp_dir)), free_mb)

    # Alert only when there are errors
    if sns_topic_arn and errors:
        try:
            send_file_transfer_sns_alert(
                sns_topic_arn,
                trace_id,
                transfer_status=transfer_status,
                checksum_status=checksum_status,
                errors=errors,
                warnings=warnings,
            )
        except Exception as e:
            log_error(trace_id, "Failed to send SNS alert", exc=e)

    missing_flags = infer_missing_flags_from_moved_files(moved_files)

    extra_payload = {
        "src_dir": src_remote_dir,
        "dest_dir": dest_remote_dir,
        "s3_bucket": s3_bucket,
        "s3_prefix": s3_prefix,
        "date_subpath": date_subpath,
        "missing_files": missing_flags,
        "import_date": import_date,
    }

    if moved_files:
        trigger_salesforce_dataloader(
            trace_id,
            moved_files,
            extra_payload=extra_payload,
        )
    else:
        logger.info(
            "[%s] No files moved; still triggering Dataloader with missing flags.",
            trace_id,
        )
        trigger_salesforce_dataloader(
            trace_id,
            moved_files,
            extra_payload=extra_payload,
        )

    log_job_end(trace_id, job_id)

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "Completed SFTP move and archive (and triggered Salesforce Dataloader).",
                "trace_id": trace_id,
                "moved_files": moved_files,
                "transfer_status": transfer_status,
                "metrics": metrics,
                "errors": errors,
                "warnings": warnings,
                "missing_flags": missing_flags,
                "date_subpath": date_subpath,
                "import_date": import_date,
            }
        ),
    }