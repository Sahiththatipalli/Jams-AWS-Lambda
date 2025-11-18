# lambda_function.py — UPDATED to derive S3 folder from YYYYMMDD in filename

import os
import json
import boto3
import paramiko
import tempfile
import shutil
import logging
import ftplib
import time

from boxsdk import JWTAuth, Client

from logging_utils import (
    log_job_start, log_job_end, log_sftp_connection, log_matched_files,
    log_checksum_ok, log_checksum_fail, log_file_transferred, log_archive,
    log_tmp_usage, log_warning, log_error, log_box_version
)
from checksum_utils import log_checksum
from trace_utils import get_or_create_trace_id
from file_match_utils import match_files
from retry_utils import default_retry
from storage_utils import (
    # Box uploader now groups by YYYYMMDD found in each filename
    upload_files_to_box_prev_bday,
    # Fallback if a file has no valid date
    get_prev_business_day_str,
    # NEW: extract date from filename for S3 foldering
    extract_yyyymmdd_from_name
)
from performance_utils import time_operation
from metrics_utils import publish_file_transfer_metric, publish_error_metric
from alert_utils import send_file_transfer_sns_alert
from dry_run_utils import is_dry_run_enabled, log_dry_run_action

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.getLogger("boxsdk").setLevel(logging.WARNING)

s3_client = boto3.client('s3')


def get_secret(secret_name):
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    secret = response['SecretString']
    return json.loads(secret)


def get_file_patterns():
    val = os.getenv('FILE_PATTERN')
    if val:
        return [x.strip() for x in val.split(',') if x.strip()]
    return ['*']


@default_retry()
def create_sftp_client(host, port, username, password):
    transport = paramiko.Transport((host, port))
    transport.connect(username=username, password=password)
    return paramiko.SFTPClient.from_transport(transport)


# ---------- SFTP -> S3 (folder from filename YYYYMMDD) ----------
@default_retry()
def download_and_upload_to_s3(
    sftp_client, remote_dir, bucket, prefix, local_dir, trace_id, job_id,
    file_patterns, metrics, transfer_status, checksum_status, errors, warnings, DRY_RUN
):
    all_files = []
    try:
        all_files = sftp_client.listdir(remote_dir)
    except Exception as e:
        err = f"SFTP listdir failed: {e}"
        log_error(trace_id, err)
        errors.append(err)
        return

    files = match_files(all_files, include_patterns=file_patterns)
    unmatched = set(all_files) - set(files)

    if not files:
        warn = "No files matched the configured FILE_PATTERN on SFTP."
        log_warning(trace_id, warn)
        warnings.append(warn)

    # NOTE: No precomputed date key; we compute per-file from its filename
    log_matched_files(trace_id, files, unmatched)

    total_bytes = 0
    t0 = time.time()

    for filename in files:
        remote_path = f"{remote_dir}/{filename}"
        local_path = os.path.join(local_dir, filename)

        try:
            _, _ = time_operation(sftp_client.get, remote_path, local_path)
            bytes_transferred = os.path.getsize(local_path)
            total_bytes += bytes_transferred

            downloaded_checksum = log_checksum(local_path, trace_id, algo="sha256", note="after SFTP download")
            s3_upload_checksum = log_checksum(local_path, trace_id, algo="sha256", note="before S3 upload")

            if downloaded_checksum == s3_upload_checksum:
                log_checksum_ok(trace_id, filename, downloaded_checksum)
                checksum_status[filename] = f"OK (sha256: {downloaded_checksum})"
            else:
                log_checksum_fail(trace_id, filename, downloaded_checksum, s3_upload_checksum)
                checksum_status[filename] = f"FAIL (downloaded: {downloaded_checksum}, s3: {s3_upload_checksum})"
                errors.append(f"Checksum mismatch for file {filename}")

            # Derive S3 folder from filename: e.g. "..._20251105182302.csv" -> "20251105"
            folder_key = extract_yyyymmdd_from_name(filename) or get_prev_business_day_str()
            s3_key = f"{prefix}/{folder_key}/{filename}" if prefix else f"{folder_key}/{filename}"

            if DRY_RUN:
                log_dry_run_action("Would upload to S3", local_path, bucket, s3_key)
            else:
                _, s3_duration = time_operation(s3_client.upload_file, local_path, bucket, s3_key)
                log_file_transferred(trace_id, filename, "S3", s3_duration)
                log_archive(trace_id, filename, s3_key)

        except Exception as e:
            err = f"Failed processing file {filename} on SFTP->S3 step: {e}"
            log_error(trace_id, err)
            errors.append(err)
            continue

    t1 = time.time()
    download_time = t1 - t0
    mb = total_bytes / 1024 / 1024 if total_bytes else 0.0
    mbps = (mb / download_time) if download_time else 0.0

    metrics["S3 upload speed mb/s"] = f"{mbps:.2f}"
    metrics["S3 total mb"] = f"{mb:.2f}"
    metrics["SFTP download speed mb/s"] = f"{mbps:.2f}"
    metrics["SFTP total mb"] = f"{mb:.2f}"

    transfer_status["s3"] = f"SUCCESS ({', '.join(files)})" if files else "NO FILES"

    if not DRY_RUN:
        try:
            publish_file_transfer_metric(
                namespace='LambdaFileTransfer',
                direction='SFTP_TO_S3',
                file_count=len(files),
                total_bytes=total_bytes,
                duration_sec=round(download_time, 2),
                trace_id=trace_id
            )
        except Exception as e:
            err = f"CloudWatch metric error for S3 transfer: {e}"
            log_error(trace_id, err)
            publish_error_metric('LambdaFileTransfer', 'S3MetricError', trace_id)
            errors.append(err)


# ---------- Helpers for external SFTP ----------
def _sftp_mkdirs_and_cd(sftp, path):
    """mkdir -p & chdir for SFTP."""
    if not path or path == '/':
        sftp.chdir('/')  # root
        return
    parts = [p for p in path.strip('/').split('/') if p]
    sftp.chdir('/')  # start at root to ensure absolute behavior
    for part in parts:
        try:
            sftp.chdir(part)
        except IOError:
            sftp.mkdir(part)
            sftp.chdir(part)


# ---------- Local -> External SFTP (NO date logic) ----------
@default_retry()
def upload_files_to_external_sftp(
    host, port, username, password, remote_dir, local_dir, trace_id, job_id,
    file_patterns, metrics, transfer_status, checksum_status, errors, warnings, DRY_RUN
):
    files = match_files(os.listdir(local_dir), include_patterns=file_patterns)
    unmatched = set(os.listdir(local_dir)) - set(files)

    if not files:
        warn = "No files matched the configured FILE_PATTERN for SFTP upload."
        log_warning(trace_id, warn)
        warnings.append(warn)

    # NEW/CONFIRMED: Do NOT add date subfolders; write directly to remote_dir
    full_path = remote_dir if remote_dir else '/'

    log_matched_files(trace_id, files, unmatched)

    sftp = None
    total_bytes = 0
    t0 = time.time()

    if DRY_RUN:
        log_dry_run_action("Would connect/login to SFTP", host, username)
    else:
        try:
            sftp = create_sftp_client(host, port, username, password)
            log_sftp_connection(trace_id, host, "OPENED")
            _sftp_mkdirs_and_cd(sftp, full_path)
        except Exception as e:
            err = f"SFTP connection/login failed: {e}"
            log_error(trace_id, err)
            errors.append(err)
            return

    for filename in files:
        local_path = os.path.join(local_dir, filename)
        try:
            sftp_upload_checksum = log_checksum(local_path, trace_id, algo="sha256", note="before SFTP upload")

            if DRY_RUN:
                log_dry_run_action("Would SFTP put", local_path, host, (full_path.rstrip('/') + '/' + filename))
            else:
                _, sftp_duration = time_operation(sftp.put, local_path, filename)
                bytes_transferred = os.path.getsize(local_path)
                total_bytes += bytes_transferred
                log_file_transferred(trace_id, filename, "SFTP", sftp_duration)

            checksum_status[filename] = f"OK (sha256: {sftp_upload_checksum})"
        except Exception as e:
            err = f"Failed SFTP upload for file {filename}: {e}"
            log_error(trace_id, err)
            errors.append(err)
            continue

    if sftp and not DRY_RUN:
        try:
            sftp.close()
            log_sftp_connection(trace_id, host, "CLOSED")
        except Exception:
            pass

    t1 = time.time()
    upload_time = t1 - t0
    mb = total_bytes / 1024 / 1024 if total_bytes else 0.0
    mbps = (mb / upload_time) if upload_time else 0.0

    metrics["External SFTP upload mb/s"] = f"{mbps:.2f}"
    metrics["External SFTP total mb"] = f"{mb:.2f}"

    transfer_status["external"] = f"SUCCESS ({', '.join(files)})" if files else "NO FILES"

    if not DRY_RUN:
        try:
            publish_file_transfer_metric(
                namespace='LambdaFileTransfer',
                direction='LOCAL_TO_SFTP',
                file_count=len(files),
                total_bytes=total_bytes,
                duration_sec=round(upload_time, 2),
                trace_id=trace_id
            )
        except Exception as e:
            err = f"CloudWatch metric error for SFTP transfer: {e}"
            log_error(trace_id, err)
            publish_error_metric('LambdaFileTransfer', 'SftpMetricError', trace_id)
            errors.append(err)


# ---------- Local -> External FTP (NO date logic) ----------
@default_retry()
def upload_files_to_external_ftp(
    ftp_host, ftp_user, ftp_pass, remote_dir, local_dir, trace_id, job_id,
    file_patterns, metrics, transfer_status, checksum_status, errors, warnings, DRY_RUN
):
    files = match_files(os.listdir(local_dir), include_patterns=file_patterns)
    unmatched = set(os.listdir(local_dir)) - set(files)

    if not files:
        warn = "No files matched the configured FILE_PATTERN for FTP upload."
        log_warning(trace_id, warn)
        warnings.append(warn)

    # NEW/CONFIRMED: Do NOT add date subfolders; write directly to remote_dir
    full_path = remote_dir if remote_dir else '/'
    parts = [p for p in full_path.strip('/').split('/') if p]

    log_matched_files(trace_id, files, unmatched)

    ftp = None
    if not DRY_RUN:
        try:
            ftp = ftplib.FTP(ftp_host)
            ftp.login(ftp_user, ftp_pass)
            if full_path == '/':
                ftp.cwd('/')
            else:
                ftp.cwd('/')
                for part in parts:
                    try:
                        ftp.mkd(part)
                    except Exception:
                        pass
                    ftp.cwd(part)
        except Exception as e:
            err = f"FTP connection/login failed: {e}"
            log_error(trace_id, err)
            errors.append(err)
            return
    else:
        log_dry_run_action("Would connect and login to FTP", ftp_host, ftp_user)

    total_bytes = 0
    t0 = time.time()

    for filename in files:
        local_path = os.path.join(local_dir, filename)
        try:
            ftp_upload_checksum = log_checksum(local_path, trace_id, algo="sha256", note="before FTP upload")

            if DRY_RUN:
                log_dry_run_action("Would upload to FTP", local_path, ftp_host, (full_path.rstrip('/') + '/'))
            else:
                with open(local_path, 'rb') as f:
                    _, ftp_duration = time_operation(ftp.storbinary, f'STOR {filename}', f)
                    bytes_transferred = os.path.getsize(local_path)
                    total_bytes += bytes_transferred
                    log_file_transferred(trace_id, filename, "FTP", ftp_duration)

            checksum_status[filename] = f"OK (sha256: {ftp_upload_checksum})"
        except Exception as e:
            err = f"Failed FTP upload for file {filename}: {e}"
            log_error(trace_id, err)
            errors.append(err)
            continue

    if ftp and not DRY_RUN:
        ftp.quit()

    t1 = time.time()
    upload_time = t1 - t0
    mb = total_bytes / 1024 / 1024 if total_bytes else 0.0
    mbps = (mb / upload_time) if upload_time else 0.0

    metrics["FTP upload mb/s"] = f"{mbps:.2f}"
    metrics["FTP total mb"] = f"{mb:.2f}"

    transfer_status["external"] = f"SUCCESS ({', '.join(files)})" if files else "NO FILES"

    if not DRY_RUN:
        try:
            publish_file_transfer_metric(
                namespace='LambdaFileTransfer',
                direction='LOCAL_TO_FTP',
                file_count=len(files),
                total_bytes=total_bytes,
                duration_sec=round(upload_time, 2),
                trace_id=trace_id
            )
        except Exception as e:
            err = f"CloudWatch metric error for FTP transfer: {e}"
            log_error(trace_id, err)
            publish_error_metric('LambdaFileTransfer', 'FtpMetricError', trace_id)
            errors.append(err)


# ---------- Handler ----------
def lambda_handler(event, context):
    trace_id = get_or_create_trace_id(context)
    job_id = trace_id
    file_patterns = get_file_patterns()

    errors = []
    warnings = []

    log_job_start(trace_id, job_id, file_patterns)

    DRY_RUN = is_dry_run_enabled()

    if os.getenv('ERROR_INJECT', 'false').lower() == 'true':
        err_msg = "Error injection test alert triggered."
        log_error(trace_id, err_msg)
        errors.append(err_msg)

    external_host_for_alert = "N/A"

    try:
        src_secret_name = os.getenv('SRC_SECRET_NAME')
        ext_secret_name = os.getenv('EXT_SECRET_NAME')
        box_secret_name = os.getenv('BOX_SECRET_NAME')
        box_folder_id = os.getenv('BOX_FOLDER_ID')

        s3_bucket = os.getenv('S3_BUCKET', 'jams-ftp-process-bucket')
        s3_prefix  = os.getenv('S3_PREFIX', 'ftp-ftp-list')
        function_name = os.getenv('AWS_LAMBDA_FUNCTION_NAME', 'N/A')

        # Source SFTP
        src_secret = get_secret(src_secret_name)
        src_host = src_secret['Host']
        src_user = src_secret['Username']
        src_pass = src_secret['Password']
        src_dir  = os.getenv('SRC_REMOTE_DIR', '.')

        # External destination (auto-select protocol)
        ext_secret = get_secret(ext_secret_name)
        external_host = ext_secret.get('host') or ext_secret.get('Host')
        external_user = ext_secret['Username']
        external_pass = ext_secret.get('password') or ext_secret.get('Password')
        external_port = int(ext_secret.get('port') or ext_secret.get('Port') or 21)
        external_dir  = os.getenv('EXT_REMOTE_DIR', '/')
        auto_proto    = 'sftp' if external_port == 22 or str(external_host).lower().startswith('sftp.') else 'ftp'
        external_protocol = os.getenv('EXT_PROTOCOL', auto_proto).lower()
        external_host_for_alert = external_host or external_host_for_alert

        logger.info(f"[{trace_id}] External target: host={external_host}, port={external_port}, protocol={external_protocol}")

        # Box
        box_jwt_config = get_secret(box_secret_name)
        auth = JWTAuth(
            client_id=box_jwt_config['boxAppSettings']['clientID'],
            client_secret=box_jwt_config['boxAppSettings']['clientSecret'],
            enterprise_id=box_jwt_config['enterpriseID'],
            jwt_key_id=box_jwt_config['boxAppSettings']['appAuth']['publicKeyID'],
            rsa_private_key_data=box_jwt_config['boxAppSettings']['appAuth']['privateKey'],
            rsa_private_key_passphrase=box_jwt_config['boxAppSettings']['appAuth']['passphrase'].encode('utf-8'),
        )
        box_client = Client(auth)

        metrics = {}
        transfer_status = {}
        checksum_status = {}

        with tempfile.TemporaryDirectory() as tmp_dir:
            free_mb = shutil.disk_usage(tmp_dir).free // (1024 * 1024)
            if free_mb < 100:
                warn = f"Low disk space on temp directory: {free_mb} MB free"
                log_warning(trace_id, warn)
                warnings.append(warn)

            log_tmp_usage(trace_id, len(os.listdir(tmp_dir)), free_mb)

            # Source SFTP
            try:
                src_sftp = create_sftp_client(src_host, 22, src_user, src_pass)
                log_sftp_connection(trace_id, src_host, "OPENED")
            except Exception as e:
                err = f"SFTP connection/login failed: {e}"
                log_error(trace_id, err)
                errors.append(err)
                raise

            # SFTP -> S3 (folder from filename's YYYYMMDD)
            download_and_upload_to_s3(
                src_sftp, src_dir, s3_bucket, s3_prefix, tmp_dir, trace_id, job_id,
                file_patterns, metrics, transfer_status, checksum_status, errors, warnings, DRY_RUN
            )
            src_sftp.close()
            log_sftp_connection(trace_id, src_host, "CLOSED")

            free_mb = shutil.disk_usage(tmp_dir).free // (1024 * 1024)
            log_tmp_usage(trace_id, len(os.listdir(tmp_dir)), free_mb)

            # Local -> External destination (NO date logic)
            if external_protocol == 'sftp':
                upload_files_to_external_sftp(
                    external_host, external_port, external_user, external_pass, external_dir,
                    tmp_dir, trace_id, job_id, file_patterns, metrics, transfer_status,
                    checksum_status, errors, warnings, DRY_RUN
                )
            else:
                upload_files_to_external_ftp(
                    external_host, external_user, external_pass, external_dir,
                    tmp_dir, trace_id, job_id, file_patterns, metrics, transfer_status,
                    checksum_status, errors, warnings, DRY_RUN
                )

            free_mb = shutil.disk_usage(tmp_dir).free // (1024 * 1024)
            log_tmp_usage(trace_id, len(os.listdir(tmp_dir)), free_mb)

            # Local -> Box (grouping by filename's YYYYMMDD handled in storage_utils)
            box_files = match_files(os.listdir(tmp_dir), include_patterns=file_patterns)
            unmatched = set(os.listdir(tmp_dir)) - set(box_files)

            if not box_files:
                warn = "No files matched FILE_PATTERN for Box, skipping Box upload."
                log_warning(trace_id, warn)
                warnings.append(warn)

            log_matched_files(trace_id, box_files, unmatched)

            try:
                if box_files:
                    box_tmp_dir = os.path.join(tmp_dir, "boxonly")
                    os.makedirs(box_tmp_dir, exist_ok=True)
                    for fname in box_files:
                        shutil.copy2(os.path.join(tmp_dir, fname), os.path.join(box_tmp_dir, fname))
                    box_total_bytes = sum(os.path.getsize(os.path.join(box_tmp_dir, f)) for f in box_files)
                    t0 = time.time()
                    if DRY_RUN:
                        log_dry_run_action(
                            "Would upload files to Box (by filename's YYYYMMDD)",
                            box_tmp_dir,
                            box_folder_id
                        )
                        transfer_status["box"] = f"SUCCESS ({', '.join(box_files)})"
                    else:
                        upload_files_to_box_prev_bday(box_client, box_folder_id, box_tmp_dir, context)
                        t1 = time.time()
                        box_upload_time = t1 - t0
                        box_mb = box_total_bytes / 1024 / 1024 if box_total_bytes else 0.0
                        box_mbps = (box_mb / box_upload_time) if box_upload_time else 0.0
                        metrics["Box upload speed mb/s"] = f"{box_mbps:.2f}"
                        metrics["Box total mb"] = f"{box_mb:.2f}"
                        transfer_status["box"] = f"SUCCESS ({', '.join(box_files)})"
                        for fname in box_files:
                            log_box_version(trace_id, fname, "box_id", "box_version")
                        log_file_transferred(trace_id, f"{len(box_files)} file(s)", "Box", box_upload_time, box_mbps)
            except Exception as e:
                err = f"Box upload failed: {e}"
                log_error(trace_id, err)
                errors.append(err)
                transfer_status["box"] = f"FAILED ({e})"

            free_mb = shutil.disk_usage(tmp_dir).free // (1024 * 1024)
            log_tmp_usage(trace_id, len(os.listdir(tmp_dir)), free_mb)

    except Exception as e:
        err = f"Unhandled exception: {e}"
        log_error(trace_id, err, exc=e)
        errors.append(err)

    # Alerts only on errors/warnings
    if errors or warnings:
        send_file_transfer_sns_alert(
            trace_id=trace_id,
            s3_files=[f for f in transfer_status.get("s3", "").replace("SUCCESS (", "").replace(")", "").split(", ") if f],
            box_files=[f for f in transfer_status.get("box", "").replace("SUCCESS (", "").replace(")", "").split(", ") if f],
            ftp_files=[f for f in transfer_status.get("external", "").replace("SUCCESS (", "").replace(")", "").split(", ") if f],
            ftp_host=external_host_for_alert,
            errors=errors,
            warnings=warnings,
            function_name=function_name
        )
    else:
        logger.info(f"[{trace_id}] No errors or warnings detected, no alert sent.")

    log_job_end(trace_id, job_id)

    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Files transferred successfully to all destinations.', 'trace_id': trace_id})
    }
