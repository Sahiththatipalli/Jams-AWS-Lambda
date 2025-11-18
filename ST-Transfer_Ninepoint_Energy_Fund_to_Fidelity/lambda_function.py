import os
import json
import boto3
import paramiko
import tempfile
import shutil
import logging
import time
import stat

from logging_utils import (
    log_job_start, log_job_end, log_sftp_connection, log_matched_files,
    log_checksum_ok, log_checksum_fail, log_file_transferred, log_archive,
    log_tmp_usage, log_warning, log_error
)
from checksum_utils import log_checksum
from trace_utils import get_or_create_trace_id
from file_match_utils import match_files
from retry_utils import default_retry
from storage_utils import get_s3_date_folder
from performance_utils import time_operation
from metrics_utils import publish_file_transfer_metric, publish_error_metric
from alert_utils import send_file_transfer_sns_alert
from dry_run_utils import is_dry_run_enabled, log_dry_run_action

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.getLogger("paramiko").setLevel(logging.WARNING)

s3_client = boto3.client('s3')

MIN_FREE_MB_THRESHOLD = 100  # warn if /tmp has less than this

def get_secret(secret_name):
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    secret = response['SecretString']
    return json.loads(secret)

def _normalize_secret(dct):
    # case-insensitive keys
    return {str(k).lower(): v for k, v in dct.items()}

def get_file_patterns():
    val = os.getenv('FILE_PATTERN')
    if val:
        return [x.strip() for x in val.split(',') if x.strip()]
    # sensible default: match both examples if no env set
    return ['NPENERGYFD_*.csv', 'Ninepoint_Energy_Positions_*.csv']

@default_retry()
def open_sftp(host, port, username, password):
    try:
        transport = paramiko.Transport((host, int(port or 22)))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        return sftp, transport
    except Exception as e:
        raise RuntimeError(f"SFTP connection error to {host}:{port}: {e}")

def _close_sftp(sftp, transport):
    try:
        if sftp:
            sftp.close()
    finally:
        try:
            if transport:
                transport.close()
        except Exception:
            pass

def _list_regular_files(sftp, remote_dir):
    """Return only regular file names from remote_dir (skip folders)."""
    entries = sftp.listdir_attr(remote_dir)
    files = [ent.filename for ent in entries if stat.S_ISREG(ent.st_mode)]
    return files

@default_retry()
def download_from_source_and_upload_to_s3(
    src_sftp, remote_dir, bucket, prefix, local_dir,
    trace_id, job_id, file_patterns, metrics, transfer_status,
    checksum_status, errors, warnings
):
    try:
        all_entries = _list_regular_files(src_sftp, remote_dir)
    except Exception as e:
        err_msg = f"Failed to list files on source SFTP {remote_dir}: {e}"
        log_error(trace_id, err_msg)
        errors.append(err_msg)
        return []

    files = match_files(all_entries, include_patterns=file_patterns)
    unmatched = [f for f in all_entries if f not in files]
    log_matched_files(trace_id, files, unmatched)

    if not files:
        warning_msg = f"No files matched patterns {file_patterns} in SFTP directory {remote_dir}"
        log_warning(trace_id, warning_msg)
        warnings.append(warning_msg)

    if is_dry_run_enabled():
        for filename in files:
            dry_msg = f"[DRY RUN] Would download {filename} and upload to S3"
            logger.info(f"[{trace_id}] {dry_msg}")
            log_dry_run_action(dry_msg)
        transfer_status["s3"] = list(files)
        return list(files)

    total_download_bytes = 0
    total_download_time = 0.0
    total_s3_bytes = 0
    total_s3_time = 0.0
    s3_uploaded_files = []

    s3_date_prefix = get_s3_date_folder()  # YYYYMMDD

    for filename in files:
        remote_path = f"{remote_dir.rstrip('/')}/{filename}"
        local_path = os.path.join(local_dir, filename)
        try:
            _, dl_dur = time_operation(src_sftp.get, remote_path, local_path)
            total_download_time += dl_dur
            bytes_downloaded = os.path.getsize(local_path)
            total_download_bytes += bytes_downloaded

            file_checksum = log_checksum(local_path, trace_id, algo="sha256", note="after SFTP download")
            # S3 key: prefix/YYYYMMDD/filename
            key_parts = []
            if prefix:
                key_parts.append(prefix.strip('/'))
            key_parts.append(s3_date_prefix)
            key_parts.append(filename)
            s3_key = "/".join(key_parts)

            # Upload to S3 (with checksum if supported)
            _, s3_dur = time_operation(
                s3_client.upload_file,
                local_path, bucket, s3_key,
                ExtraArgs={"ChecksumAlgorithm": "SHA256"}
            )
            total_s3_time += s3_dur
            total_s3_bytes += bytes_downloaded
            s3_uploaded_files.append(filename)

            log_file_transferred(trace_id, filename, "S3", s3_dur)
            log_archive(trace_id, filename, s3_key)
            log_checksum_ok(trace_id, filename, file_checksum)
            checksum_status[filename] = f"OK (sha256: {file_checksum})"
        except Exception as e:
            err_msg = f"Failed SFTP->S3 for {filename}: {e}"
            log_error(trace_id, err_msg)
            errors.append(err_msg)

    # metrics
    mb_dl = total_download_bytes / 1024 / 1024 if total_download_bytes else 0.0
    mbps_dl = (mb_dl / total_download_time) if total_download_time else 0.0
    metrics["SFTP download speed mb/s"] = f"{mbps_dl:.2f}"
    metrics["SFTP total mb"] = f"{mb_dl:.2f}"

    mb_s3 = total_s3_bytes / 1024 / 1024 if total_s3_bytes else 0.0
    mbps_s3 = (mb_s3 / total_s3_time) if total_s3_time else 0.0
    metrics["S3 upload speed mb/s"] = f"{mbps_s3:.2f}"
    metrics["S3 total mb"] = f"{mb_s3:.2f}"

    transfer_status["s3"] = list(s3_uploaded_files)

    try:
        publish_file_transfer_metric(
            namespace='LambdaFileTransfer',
            direction='SFTP_TO_S3',
            file_count=len(s3_uploaded_files),
            total_bytes=total_s3_bytes,
            duration_sec=round(total_s3_time, 2),
            trace_id=trace_id
        )
    except Exception as e:
        err_msg = f"CloudWatch metric error for S3 transfer: {e}"
        log_error(trace_id, err_msg)
        publish_error_metric('LambdaFileTransfer', 'S3MetricError', trace_id)
        errors.append(err_msg)

    return list(files)  # return list of files that were downloaded

@default_retry()
def upload_files_to_external_sftp(
    dest_host, dest_port, dest_user, dest_pass, remote_dir, local_dir,
    trace_id, job_id, file_patterns, metrics, transfer_status,
    checksum_status, errors, warnings, dry_run_files=None, ftp_server_name=None
):
    server_display = ftp_server_name or dest_host
    logger.info(f"[{trace_id}] Starting upload to external SFTP: {server_display}")

    files = dry_run_files if is_dry_run_enabled() and dry_run_files is not None \
        else match_files(os.listdir(local_dir), include_patterns=file_patterns)

    unmatched = [f for f in os.listdir(local_dir) if f not in files]
    log_matched_files(trace_id, files, unmatched)

    if not files:
        warning_msg = f"No files matched patterns {file_patterns} in local dir {local_dir} for external SFTP upload"
        log_warning(trace_id, warning_msg)
        warnings.append(warning_msg)

    # temp disk free warning
    free_mb = shutil.disk_usage(local_dir).free // (1024 * 1024)
    if free_mb < MIN_FREE_MB_THRESHOLD:
        warning_msg = f"Low disk space on temp directory: {free_mb} MB free"
        log_warning(trace_id, warning_msg)
        warnings.append(warning_msg)

    if is_dry_run_enabled():
        for filename in files:
            dry_msg = f"[DRY RUN] Would upload {filename} to external SFTP {server_display} at {remote_dir}"
            logger.info(f"[{trace_id}] {dry_msg}")
            log_dry_run_action(dry_msg)
        transfer_status["ftp"] = list(files)  # keep key name 'ftp' for alert_utils compatibility
        return

    try:
        dest_sftp, dest_transport = open_sftp(dest_host, dest_port, dest_user, dest_pass)
        log_sftp_connection(trace_id, dest_host, "OPENED (DEST)")
    except Exception as e:
        err_msg = f"Failed to connect to external SFTP {dest_host}: {e}"
        log_error(trace_id, err_msg)
        errors.append(err_msg)
        return

    # Ensure remote_dir exists & cd there (no date folder per requirements)
    try:
        target_dir = remote_dir.strip() or '/'
        if target_dir != '/':
            # Create nested path if needed
            parts = [p for p in target_dir.strip('/').split('/') if p]
            cur = '/'
            try:
                dest_sftp.chdir('/')
            except IOError:
                # some servers may not allow explicit '/'
                pass
            for part in parts:
                cur = f"{cur.rstrip('/')}/{part}"
                try:
                    dest_sftp.chdir(cur)
                except IOError:
                    try:
                        dest_sftp.mkdir(cur)
                    except Exception:
                        pass
                    dest_sftp.chdir(cur)
        else:
            try:
                dest_sftp.chdir('/')
            except IOError:
                # ignore if server uses home as '/'
                pass
    except Exception as e:
        err_msg = f"Failed to navigate to external SFTP directory '{remote_dir}': {e}"
        log_error(trace_id, err_msg)
        errors.append(err_msg)
        _close_sftp(dest_sftp, dest_transport)
        return

    total_bytes = 0
    total_time = 0.0
    uploaded = []

    for filename in files:
        local_path = os.path.join(local_dir, filename)
        remote_path = filename if (remote_dir.strip() in ['', '/']) else f"{remote_dir.rstrip('/')}/{filename}"
        try:
            file_checksum = log_checksum(local_path, trace_id, algo="sha256", note="before external SFTP upload")
            _, up_dur = time_operation(dest_sftp.put, local_path, remote_path)
            total_time += up_dur
            bytes_transferred = os.path.getsize(local_path)
            total_bytes += bytes_transferred
            uploaded.append(filename)
            log_file_transferred(trace_id, filename, "SFTP", up_dur)
            checksum_status[filename] = f"OK (sha256: {file_checksum})"
        except Exception as e:
            err_msg = f"Failed to upload {filename} to external SFTP: {e}"
            log_error(trace_id, err_msg)
            errors.append(err_msg)
            continue

    try:
        _close_sftp(dest_sftp, dest_transport)
        log_sftp_connection(trace_id, dest_host, "CLOSED (DEST)")
    except Exception as e:
        log_warning(trace_id, f"Failed to close external SFTP cleanly: {e}")

    mb = total_bytes / 1024 / 1024 if total_bytes else 0.0
    mbps = (mb / total_time) if total_time else 0.0
    metrics["External SFTP upload speed mb/s"] = f"{mbps:.2f}"
    metrics["External SFTP total mb"] = f"{mb:.2f}"

    transfer_status["ftp"] = list(uploaded)  # keep key 'ftp' for alert payload

    try:
        publish_file_transfer_metric(
            namespace='LambdaFileTransfer',
            direction='LOCAL_TO_SFTP',
            file_count=len(uploaded),
            total_bytes=total_bytes,
            duration_sec=round(total_time, 2),
            trace_id=trace_id
        )
    except Exception as e:
        err_msg = f"CloudWatch metric error for external SFTP: {e}"
        log_error(trace_id, err_msg)
        publish_error_metric('LambdaFileTransfer', 'DestSftpMetricError', trace_id)
        errors.append(err_msg)

def lambda_handler(event, context):
    """
    Environment variables expected:
      SRC_SECRET_NAME  (e.g., 'sftp/ninepoint/credentials')
      EXT_SECRET_NAME  (e.g., 'sftp/fidelity/credentials')
      SRC_REMOTE_DIR   (e.g., '/cibc/mellon_prod_files/')
      EXT_REMOTE_DIR   (e.g., '/')  # root on external SFTP
      FILE_PATTERN     (comma-separated, e.g., 'NPENERGYFD_*.csv,Ninepoint_Energy_Positions_*.csv')
      S3_BUCKET        (e.g., 'jams-ftp-process-bucket')
      S3_PREFIX        (e.g., 'fidelity/np-energy')  # objects stored at S3_PREFIX/YYYYMMDD/filename
    """
    function_name = context.function_name if context else "unknown_function"
    trace_id = get_or_create_trace_id(context)
    job_id = trace_id
    file_patterns = get_file_patterns()
    log_job_start(trace_id, job_id, file_patterns)

    from dry_run_utils import is_dry_run_enabled, get_dry_run_components
    dry = is_dry_run_enabled(event)
    comps = ",".join(sorted(get_dry_run_components())) if dry else "(disabled)"
    logger.info(f"[{trace_id}] DRY_RUN={dry} components={comps}")

    src_secret_name = os.getenv('SRC_SECRET_NAME', 'sftp/ninepoint/credentials')
    ext_secret_name = os.getenv('EXT_SECRET_NAME', 'sftp/fidelity/credentials')
    s3_bucket = os.getenv('S3_BUCKET', 'jams-ftp-process-bucket')
    s3_prefix = os.getenv('S3_PREFIX', 'fidelity/np-energy')

    src_dir = os.getenv('SRC_REMOTE_DIR', '/cibc/mellon_prod_files/')
    ext_dir = os.getenv('EXT_REMOTE_DIR', '/')  # per requirement: uploads go to root by default

    src_secret = _normalize_secret(get_secret(src_secret_name))
    src_host = src_secret.get('host', 'sftp.ninepoint.com')
    src_user = src_secret.get('username')
    src_pass = src_secret.get('password')
    src_port = int(src_secret.get('port', 22))

    ext_secret = _normalize_secret(get_secret(ext_secret_name))
    external_sftp_host = ext_secret.get('host', 'sftp-na4-win.advent.com')
    external_sftp_user = ext_secret.get('username')
    external_sftp_pass = ext_secret.get('password')
    external_sftp_port = int(ext_secret.get('port', 22))
    external_server_name = ext_secret.get('server_name', external_sftp_host)

    logger.info(f"[{trace_id}] Function {function_name} started. External SFTP: {external_server_name}")

    metrics = {}
    transfer_status = {"s3": [], "ftp": []}  # ftp = external SFTP for alert_utils compatibility
    checksum_status = {}
    errors = []
    warnings = []

    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            free_mb = shutil.disk_usage(tmp_dir).free // (1024 * 1024)
            log_tmp_usage(trace_id, len(os.listdir(tmp_dir)), free_mb)

            # Connect source SFTP
            try:
                src_sftp, src_transport = open_sftp(src_host, src_port, src_user, src_pass)
                log_sftp_connection(trace_id, src_host, "OPENED (SRC)")
            except Exception as e:
                err_msg = f"Failed to connect to source SFTP {src_host}: {e}"
                log_error(trace_id, err_msg)
                errors.append(err_msg)
                return {
                    'statusCode': 500,
                    'body': json.dumps({'error': err_msg, 'trace_id': trace_id})
                }

            # Download from source and push to S3
            dry_run_files = download_from_source_and_upload_to_s3(
                src_sftp, src_dir, s3_bucket, s3_prefix, tmp_dir, trace_id, job_id,
                file_patterns, metrics, transfer_status, checksum_status, errors, warnings
            )

            # Close source
            _close_sftp(src_sftp, src_transport)
            log_sftp_connection(trace_id, src_host, "CLOSED (SRC)")

            free_mb = shutil.disk_usage(tmp_dir).free // (1024 * 1024)
            log_tmp_usage(trace_id, len(os.listdir(tmp_dir)), free_mb)

            # Upload to external SFTP (root by default, no date folder)
            upload_files_to_external_sftp(
                external_sftp_host, external_sftp_port, external_sftp_user, external_sftp_pass,
                ext_dir, tmp_dir, trace_id, job_id, file_patterns, metrics, transfer_status,
                checksum_status, errors, warnings, dry_run_files=dry_run_files,
                ftp_server_name=external_server_name
            )

            free_mb = shutil.disk_usage(tmp_dir).free // (1024 * 1024)
            log_tmp_usage(trace_id, len(os.listdir(tmp_dir)), free_mb)

        if errors or warnings:
            send_file_transfer_sns_alert(
                trace_id=trace_id,
                s3_files=list(transfer_status.get("s3", [])),
                ftp_files=list(transfer_status.get("ftp", [])),
                checksum_results=[{'file': k, 'status': v} for k, v in checksum_status.items()],
                errors=errors,
                warnings=warnings,
                function_name=function_name,
                ftp_server_name=external_server_name
            )
        else:
            logger.info(f"[{trace_id}] No errors or warnings detected. No alert sent.")
    except Exception as e:
        err_msg = f"Unhandled exception in Lambda: {e}"
        log_error(trace_id, err_msg, exc=e)
        send_file_transfer_sns_alert(
            trace_id=trace_id,
            s3_files=list(transfer_status.get("s3", [])),
            ftp_files=list(transfer_status.get("ftp", [])),
            checksum_results=[{'file': k, 'status': v} for k, v in checksum_status.items()],
            errors=[err_msg],
            warnings=[],
            function_name=function_name,
            ftp_server_name=external_server_name
        )
        raise

    log_job_end(trace_id, job_id)
    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Files transferred to S3 (YYYYMMDD folder) and external SFTP root.', 'trace_id': trace_id})
    }
