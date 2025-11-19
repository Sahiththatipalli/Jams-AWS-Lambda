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
    get_business_day_str,
    upload_files_to_box_for_day,
    upload_files_to_box_prev_bday,   # kept for other jobs and duties'
    upload_sam_output_to_box,        # NEW
    upload_pe_output_to_box          # NEW
)
from performance_utils import time_operation
from metrics_utils import publish_file_transfer_metric, publish_error_metric
from alert_utils import send_file_transfer_sns_alert
from dry_run_utils import is_dry_run_enabled, log_dry_run_action

# GCP helpers function
from gcp_utils import (
    make_gcs_client_from_secret,
    s3_to_gcs_resilient,
    gcs_to_s3,
    wait_for_gcs_objects,
    call_http_cloud_function,
    gcs_put_smoke_test
)

# Email helpers (SMTP + SES)
from email_utils import send_email_with_attachment, parse_recipients, send_email_via_smtp

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
    """
    Build match patterns for the *source* files.
    FILE_PATTERN supports token {YYYYMMDD}.
    Picking date is controlled by DATE_MODE ('today' default, or 'prev').
    """
    val = os.getenv('FILE_PATTERN')
    patterns = [x.strip() for x in val.split(',')] if val else ['*']
    pick_mode = os.getenv('DATE_MODE', 'today').lower()
    pick_day_key = get_business_day_str(pick_mode)  # 'YYYYMMDD' for matching
    return [p.replace('{YYYYMMDD}', pick_day_key) for p in patterns if p]


@default_retry()
def create_sftp_client(host, port, username, password):
    transport = paramiko.Transport((host, port))
    transport.connect(username=username, password=password)
    return paramiko.SFTPClient.from_transport(transport)


# ---------- SFTP -> S3 (store dated by STORE_DATE_MODE) ----------
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

    # Where we STORE (foldering) — defaults to DATE_MODE for backward compat
    store_mode = os.getenv('STORE_DATE_MODE', os.getenv('DATE_MODE', 'today')).lower()
    store_day_key = get_business_day_str(store_mode)  # YYYYMMDD

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

            # STORE under store_day_key
            s3_key = f"{prefix}/{store_day_key}/{filename}" if prefix else f"{store_day_key}/{filename}"

            if DRY_RUN:
                log_dry_run_action("Would upload to S3", local_path, bucket, s3_key)
            else:
                _, s3_duration = time_operation(s3_client.upload_file, local_path, bucket, s3_key)
                log_file_transferred(trace_id, filename, "S3", s3_duration)
                log_archive(trace_id, filename, s3_key)

                # ✨ Delete source SFTP after successful upload (opt-in)
                if os.getenv("DELETE_SRC_AFTER_UPLOAD", "false").lower() == "true":
                    try:
                        sftp_client.remove(remote_path)
                        logger.info(f"[{trace_id}] Deleted source SFTP file: {remote_path}")
                    except Exception as e:
                        log_warning(trace_id, f"Could not delete source SFTP file '{remote_path}': {e}")

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


# ---------- SFTP helpers (portable chroot-safe) ----------
def _sftp_mkdirs_and_cd_portable(sftp, path: str | None, *, create: bool = True):
    """
    If create=True  -> mkdir -p then cd into the path.
    If create=False -> only cd; fail if any segment is missing.
    """
    if not path or path.strip() in (".", "/"):
        return
    parts = [p for p in path.strip("/").split("/") if p]
    for part in parts:
        if create:
            try:
                sftp.chdir(part)
            except IOError:
                sftp.mkdir(part)
                sftp.chdir(part)
        else:
            # cd only; raise if missing
            sftp.chdir(part)


# ---------- Local -> External SFTP ----------
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
            create_remote = os.getenv("EXT_CREATE_REMOTE_PATH", "true").lower() == "true"
            _sftp_mkdirs_and_cd_portable(sftp, remote_dir, create=create_remote)
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
                target = (remote_dir.rstrip('/') + '/' + filename) if remote_dir and remote_dir not in ('.', '/') else filename
                log_dry_run_action("Would SFTP put", local_path, host, target)
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


# ---------- Local -> External FTP ----------
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

    full_path = remote_dir if remote_dir else '/'
    parts = [p for p in full_path.strip('/').split('/') if p]

    log_matched_files(trace_id, files, unmatched)

    ftp = None
    if not DRY_RUN:
        try:
            ftp = ftplib.FTP(ftp_host)
            ftp.login(ftp_user, ftp_pass)
            create_remote = os.getenv("EXT_CREATE_REMOTE_PATH", "true").lower() == "true"
            ftp.cwd('/')
            if full_path != '/':
                for part in parts:
                    if create_remote:
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
    file_patterns = get_file_patterns()  # uses DATE_MODE for matching

    # ensure these exist even if we fail early
    errors = []
    warnings = []
    metrics = {}
    transfer_status = {}
    checksum_status = {}

    log_job_start(trace_id, job_id, file_patterns)
    DRY_RUN = is_dry_run_enabled()

    if os.getenv('ERROR_INJECT', 'false').lower() == 'true':
        err_msg = "Error injection test alert triggered."
        log_error(trace_id, err_msg)
        errors.append(err_msg)

    external_host_for_alert = "N/A"

    try:
        # -------- Env / secrets --------
        src_secret_name  = os.getenv('SRC_SECRET_NAME')
        ext_secret_name  = os.getenv('EXT_SECRET_NAME')          # Sprott
        box_secret_name  = os.getenv('BOX_SECRET_NAME')
        box_folder_id    = os.getenv('BOX_FOLDER_ID')            # originals root

        # NEW: separate Box roots for outputs
        box_sam_folder_id = os.getenv('BOX_SAM_FOLDER_ID')       # SAM path root (yyyy/MM/yyyyMMdd)
        box_pe_folder_id  = os.getenv('BOX_PE_FOLDER_ID')        # PE  path root (yyyy/yyyyMMdd)

        s3_bucket        = os.getenv('S3_BUCKET', 'ninepoint-files-prod')
        s3_prefix        = os.getenv('S3_PREFIX', 'ST-Transfer-NPP-Aladdin-Capstock')
        function_name    = os.getenv('AWS_LAMBDA_FUNCTION_NAME', 'N/A')

        # GCP env
        gcs_secret_name  = os.getenv('GCS_HMAC_SECRET_NAME')
        gcs_bucket       = os.getenv('GCS_BUCKET', 'npp-aladdin-bucket')
        gcf_pe_url       = os.getenv('GCF_HTTP_PE_URL')          # optional
        gcf_sprott_url   = os.getenv('GCF_HTTP_SPROTT_URL')      # optional
        gcf_bearer       = os.getenv('GCF_BEARER_TOKEN')         # optional
        out_s3_bucket    = os.getenv('OUT_S3_BUCKET', s3_bucket)
        out_s3_prefix    = os.getenv('OUT_S3_PREFIX', 'capstock/outputs')
        pe_output_name   = os.getenv('PE_OUTPUT_NAME', 'PE Sub Red.csv')
        sam_output_name  = os.getenv('SAM_OUTPUT_NAME', 'SAM Sub Red.csv')
        gcf_timeout_sec  = int(os.getenv('GCF_OUTPUT_TIMEOUT_SEC', '900'))
        gcf_poll_sec     = int(os.getenv('GCF_POLL_INTERVAL_SEC', '10'))

        # Email env (supports SMTP or SES)
        email_transport  = os.getenv("EMAIL_TRANSPORT", "ses").lower()  # 'smtp' or 'ses'
        ses_region       = os.getenv("SES_REGION", "us-east-1")
        ses_sender       = os.getenv("SES_SENDER", "notifications@ninepoint.com")
        sam_email_to     = parse_recipients(os.getenv("SAM_EMAIL_TO", "fundops@ninepoint.com"))
        pe_email_to      = parse_recipients(os.getenv("PE_EMAIL_TO",
                                 "fundops@ninepoint.com, operations@peinvestments.com, csalsman@peinvestments.com, ahartshorn@peinvestments.com, statements@peinvestments.com"))
        sam_subj         = os.getenv("SAM_EMAIL_SUBJECT", "Ninepoint Partners - DATA VALIDATION FOR SPROTT")
        sam_body_plain   = os.getenv("SAM_EMAIL_BODY_PLAIN",
                            "Hello,\n\nPlease find attached Subscriptions and Redemptions report.\n\n"
                            "If you are experiencing a support issue or have any other needs for this report, please reply all to this email.\n\nThanks")
        sam_body_html    = os.getenv("SAM_EMAIL_BODY_HTML", None)

        pe_subj          = os.getenv("PE_EMAIL_SUBJECT", "Ninepoint Partners - Subscriptions and Redemptions for PE")
        pe_body_plain    = os.getenv("PE_EMAIL_BODY_PLAIN",
                            "Hello,\n\nPlease find attached PE Subscriptions and Redemptions report.\n\n"
                            "If you are experiencing a support issue or have any other needs for this report, please reply all to this email.\n\nThanks")
        pe_body_html     = os.getenv("PE_EMAIL_BODY_HTML", None)

        # SMTP details (used if EMAIL_TRANSPORT=smtp)
        smtp_host        = os.getenv("SMTP_HOST", "10.0.31.212")
        smtp_port        = int(os.getenv("SMTP_PORT", "25"))
        smtp_from        = os.getenv("SMTP_FROM", ses_sender)  # e.g., autonotification@ninepoint.com
        smtp_starttls    = os.getenv("SMTP_STARTTLS", "false").lower() == "true"
        smtp_user        = os.getenv("SMTP_USERNAME")
        smtp_pass        = os.getenv("SMTP_PASSWORD")
        smtp_helo        = os.getenv("SMTP_HELO")
        smtp_dsn_notify  = os.getenv("SMTP_DSN_NOTIFY", "SUCCESS,FAILURE")
        smtp_dsn_ret     = os.getenv("SMTP_DSN_RET", "HDRS")

        # Optional GCS smoke test
        if os.getenv("RUN_GCS_SMOKE_TEST", "false").lower() == "true":
            res = gcs_put_smoke_test(gcs_secret_name, gcs_bucket)
            logger.info(f"GCS smoke result: {res}")

        # Source SFTP
        src_secret = get_secret(src_secret_name)
        src_host = src_secret['Host']
        src_user = src_secret['Username']
        src_pass = src_secret['Password']
        src_port = int(src_secret.get('Port') or 22)
        src_dir  = os.getenv('SRC_REMOTE_DIR', '.')

        # External destination (Sprott)
        ext_secret = get_secret(ext_secret_name)
        external_host = ext_secret.get('host') or ext_secret.get('Host')
        external_user = ext_secret['Username']
        external_pass = ext_secret.get('password') or ext_secret.get('Password')
        external_port = int(ext_secret.get('port') or ext_secret.get('Port') or 21)
        external_dir  = os.getenv('EXT_REMOTE_DIR', '.')
        auto_proto    = 'sftp' if external_port == 22 or str(external_host).lower().startswith('sftp.') else 'ftp'
        external_protocol = os.getenv('EXT_PROTOCOL', auto_proto).lower()
        external_host_for_alert = external_host or external_host_for_alert

        logger.info(f"[{trace_id}] External (final delivery) target: host={external_host}, port={external_port}, protocol={external_protocol}")

        # Box
        box_client = None
        if box_secret_name and (box_folder_id or box_sam_folder_id or box_pe_folder_id):
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

        # Where we STORE (for all downstream paths)
        store_mode = os.getenv('STORE_DATE_MODE', os.getenv('DATE_MODE', 'today')).lower()
        store_day_key = get_business_day_str(store_mode)

        with tempfile.TemporaryDirectory() as tmp_dir:
            free_mb = shutil.disk_usage(tmp_dir).free // (1024 * 1024)
            if free_mb < 100:
                warn = f"Low disk space on temp directory: {free_mb} MB free"
                log_warning(trace_id, warn)
                warnings.append(warn)

            log_tmp_usage(trace_id, len(os.listdir(tmp_dir)), free_mb)

            # Source SFTP connect
            try:
                src_sftp = create_sftp_client(src_host, src_port, src_user, src_pass)
                log_sftp_connection(trace_id, src_host, "OPENED")
            except Exception as e:
                err = f"SFTP connection/login failed: {e}"
                log_error(trace_id, err)
                errors.append(err)
                raise

            # 1) SFTP -> S3 (dated by STORE_DATE_MODE), files land in tmp_dir too
            download_and_upload_to_s3(
                src_sftp, src_dir, s3_bucket, s3_prefix, tmp_dir, trace_id, job_id,
                file_patterns, metrics, transfer_status, checksum_status, errors, warnings, DRY_RUN
            )
            src_sftp.close()
            log_sftp_connection(trace_id, src_host, "CLOSED")

            free_mb = shutil.disk_usage(tmp_dir).free // (1024 * 1024)
            log_tmp_usage(trace_id, len(os.listdir(tmp_dir)), free_mb)

            # Decide if we proceed downstream
            files_for_gcs = match_files(os.listdir(tmp_dir), include_patterns=file_patterns)

            if not files_for_gcs:
                logger.info(f"[{trace_id}] No source files were transferred; "
                            "skipping GCS processing, copy-back, emails, Box upload, and final delivery.")
                transfer_status["pipeline"] = "SKIPPED_NO_SOURCE"
            else:
                # 2) S3 -> GCS (upload original(s) to GCS), optional GCF invoke, copy outputs back to S3
                gcs_client = make_gcs_client_from_secret(gcs_secret_name)

                for fname in files_for_gcs:
                    s3_src_key = f"{s3_prefix}/{store_day_key}/{fname}"
                    if DRY_RUN:
                        log_dry_run_action("Would copy S3 -> GCS", f"s3://{s3_bucket}/{s3_src_key}", f"gs://{gcs_bucket}/{fname}")
                    else:
                        s3_to_gcs_resilient(gcs_secret_name, s3_bucket, s3_src_key, gcs_bucket, fname, s3_client)

                if gcf_pe_url:
                    try:
                        call_http_cloud_function(gcf_pe_url, {"files": files_for_gcs, "date": store_day_key}, gcf_bearer)
                    except Exception as e:
                        log_warning(trace_id, f"pe-capstock HTTP call failed (continuing): {e}")
                if gcf_sprott_url:
                    try:
                        call_http_cloud_function(gcf_sprott_url, {"files": files_for_gcs, "date": store_day_key}, gcf_bearer)
                    except Exception as e:
                        log_warning(trace_id, f"sprott-capstock HTTP call failed (continuing): {e}")

                # Wait for outputs in GCS
                if not DRY_RUN:
                    found = wait_for_gcs_objects(
                        gcs_client, gcs_bucket, [pe_output_name, sam_output_name],
                        timeout_sec=gcf_timeout_sec, poll_interval_sec=gcf_poll_sec
                    )
                else:
                    found = {pe_output_name: True, sam_output_name: True}

                # Copy back to S3 and remember which ones actually made it
                copied = {pe_output_name: False, sam_output_name: False}

                for name in [pe_output_name, sam_output_name]:
                    out_key = f"{out_s3_prefix}/{store_day_key}/{name}"
                    if DRY_RUN:
                        log_dry_run_action("Would copy GCS -> S3", f"gs://{gcs_bucket}/{name}", f"s3://{out_s3_bucket}/{out_key}")
                        copied[name] = True
                    else:
                        if found.get(name, False):
                            try:
                                gcs_to_s3(gcs_bucket, name, out_s3_bucket, out_key, gcs_client, s3_client)
                                copied[name] = True
                                logger.info(f"[{trace_id}] Copied back to S3: s3://{out_s3_bucket}/{out_key}")
                            except Exception as e:
                                log_warning(trace_id, f"Copy-back failed for {name}: {e}")
                        else:
                            log_warning(trace_id, f"Skipping copy-back for {name} (not present in GCS)")

                # 3) Email the two outputs — only if they were copied back to S3 in this run
                try:
                    if os.getenv("ENABLE_EMAIL", "true").lower() == "true":
                        pe_key  = f"{out_s3_prefix}/{store_day_key}/{pe_output_name}"
                        sam_key = f"{out_s3_prefix}/{store_day_key}/{sam_output_name}"
                        pe_path  = os.path.join(tmp_dir, pe_output_name)
                        sam_path = os.path.join(tmp_dir, sam_output_name)

                        if not DRY_RUN:
                            if copied.get(pe_output_name, False):
                                s3_client.download_file(out_s3_bucket, pe_key, pe_path)
                            if copied.get(sam_output_name, False):
                                s3_client.download_file(out_s3_bucket, sam_key, sam_path)

                        # SAM email
                        if copied.get(sam_output_name, False):
                            if DRY_RUN:
                                log_dry_run_action("Would email SAM report",
                                                   smtp_from if email_transport=="smtp" else ses_sender, sam_email_to)
                            else:
                                if email_transport == "smtp":
                                    mid = send_email_via_smtp(
                                        smtp_host=smtp_host, smtp_port=smtp_port,
                                        sender=smtp_from, to_addrs=sam_email_to,
                                        subject=sam_subj, body_text=sam_body_plain, body_html=sam_body_html,
                                        attachments=[sam_path], use_starttls=smtp_starttls,
                                        username=smtp_user, password=smtp_pass, helo_host=smtp_helo,
                                        dsn_notify=smtp_dsn_notify, dsn_ret=smtp_dsn_ret
                                    )
                                else:
                                    mid = send_email_with_attachment(ses_region, ses_sender, sam_email_to,
                                                                     sam_subj, sam_body_plain, sam_path, sam_output_name)
                                logger.info(f"[{trace_id}] SAM email MessageId={mid}")
                        else:
                            logger.info(f"[{trace_id}] SAM email suppressed (not copied back this run).")

                        # PE email
                        if copied.get(pe_output_name, False):
                            if DRY_RUN:
                                log_dry_run_action("Would email PE report",
                                                   smtp_from if email_transport=="smtp" else ses_sender, pe_email_to)
                            else:
                                if email_transport == "smtp":
                                    mid = send_email_via_smtp(
                                        smtp_host=smtp_host, smtp_port=smtp_port,
                                        sender=smtp_from, to_addrs=pe_email_to,
                                        subject=pe_subj, body_text=pe_body_plain, body_html=pe_body_html,
                                        attachments=[pe_path], use_starttls=smtp_starttls,
                                        username=smtp_user, password=smtp_pass, helo_host=smtp_helo,
                                        dsn_notify=smtp_dsn_notify, dsn_ret=smtp_dsn_ret
                                    )
                                else:
                                    mid = send_email_with_attachment(ses_region, ses_sender, pe_email_to,
                                                                     pe_subj, pe_body_plain, pe_path, pe_output_name)
                                logger.info(f"[{trace_id}] PE email MessageId={mid}")
                        else:
                            logger.info(f"[{trace_id}] PE email suppressed (not copied back this run).")

                except Exception as e:
                    err = f"Email delivery failed: {e}"
                    log_error(trace_id, err)
                    errors.append(err)

                # ✨ Delete GCS originals we uploaded this run (opt-in; after CF+copy-back)
                if os.getenv("DELETE_GCS_AFTER_PROCESS", "false").lower() == "true":
                    for fname in files_for_gcs:
                        try:
                            if DRY_RUN:
                                log_dry_run_action("Would delete GCS original", f"gs://{gcs_bucket}/{fname}", "")
                            else:
                                gcs_client.delete_object(Bucket=gcs_bucket, Key=fname)
                                logger.info(f"[{trace_id}] Deleted GCS original: gs://{gcs_bucket}/{fname}")
                        except Exception as e:
                            log_warning(trace_id, f"Could not delete GCS original '{fname}': {e}")

                # 4) (Optional) Box upload of originals — using same store_day_key
                if box_client and box_folder_id:
                    box_files = match_files(os.listdir(tmp_dir), include_patterns=file_patterns)
                    unmatched = set(os.listdir(tmp_dir)) - set(box_files)
                    if not box_files:
                        warn = "No files matched FILE_PATTERN for Box, skipping Box upload."
                        log_warning(trace_id, warn)
                        warnings.append(warn)
                    log_matched_files(trace_id, box_files, unmatched)
                    try:
                        if box_files and not DRY_RUN:
                            box_tmp_dir = os.path.join(tmp_dir, "boxonly")
                            os.makedirs(box_tmp_dir, exist_ok=True)
                            for fname in box_files:
                                shutil.copy2(os.path.join(tmp_dir, fname), os.path.join(box_tmp_dir, fname))
                            t0 = time.time()
                            upload_files_to_box_for_day(box_client, box_folder_id, box_tmp_dir, store_day_key, context)
                            t1 = time.time()
                            box_upload_time = t1 - t0
                            log_file_transferred(trace_id, f"{len(box_files)} file(s)", "Box", box_upload_time)
                            for fname in box_files:
                                log_box_version(trace_id, fname, "box_id", "box_version")
                            transfer_status["box"] = f"SUCCESS ({', '.join(box_files)})"
                        elif DRY_RUN:
                            log_dry_run_action("Would upload files to Box (day folder)", "tmp://boxonly", f"{box_folder_id}/{store_day_key}")
                            transfer_status["box"] = f"SUCCESS ({', '.join(box_files)})"
                    except Exception as e:
                        err = f"Box upload failed: {e}"
                        log_error(trace_id, err)
                        errors.append(err)
                        transfer_status["box"] = f"FAILED ({e})"

                # 4b) NEW: Upload PE/SAM outputs to their Box hierarchies
                if box_client and (box_sam_folder_id or box_pe_folder_id):
                    pe_path  = os.path.join(tmp_dir, pe_output_name)
                    sam_path = os.path.join(tmp_dir, sam_output_name)

                    # If not in DRY_RUN and copied, the files were downloaded above for email.
                    # If DRY_RUN, just log preview actions.

                    if copied.get(sam_output_name, False) and box_sam_folder_id:
                        if DRY_RUN:
                            log_dry_run_action("Would upload SAM to Box hierarchy yyyy/MM/yyyyMMdd",
                                               sam_output_name, box_sam_folder_id)
                        else:
                            try:
                                upload_sam_output_to_box(box_client, box_sam_folder_id, sam_path, store_day_key, context)
                                logger.info(f"[{trace_id}] SAM uploaded to Box output hierarchy.")
                            except Exception as e:
                                log_warning(trace_id, f"SAM Box upload (hierarchy) failed: {e}")

                    if copied.get(pe_output_name, False) and box_pe_folder_id:
                        if DRY_RUN:
                            log_dry_run_action("Would upload PE to Box hierarchy yyyy/yyyyMMdd",
                                               pe_output_name, box_pe_folder_id)
                        else:
                            try:
                                upload_pe_output_to_box(box_client, box_pe_folder_id, pe_path, store_day_key, context)
                                logger.info(f"[{trace_id}] PE uploaded to Box output hierarchy.")
                            except Exception as e:
                                log_warning(trace_id, f"PE Box upload (hierarchy) failed: {e}")

                # 5) Final delivery: only SAM Sub Red.csv from S3 -> Sprott (SFTP/FTP) — only if copied back this run
                sam_s3_key = f"{out_s3_prefix}/{store_day_key}/{sam_output_name}"
                local_sam  = os.path.join(tmp_dir, sam_output_name)

                if 'copied' in locals() and copied.get(sam_output_name, False):
                    if DRY_RUN:
                        open(local_sam, "wb").close()  # DRY shim
                        log_dry_run_action("Would download final from S3 for Sprott delivery",
                                           f"s3://{out_s3_bucket}/{sam_s3_key}", local_sam)
                    else:
                        try:
                            s3_client.download_file(out_s3_bucket, sam_s3_key, local_sam)
                            logger.info(f"[{trace_id}] Downloaded from S3 for final delivery: s3://{out_s3_bucket}/{sam_s3_key}")
                        except Exception as e:
                            err = f"Failed to download final output for delivery: {e}"
                            log_error(trace_id, err)
                            errors.append(err)

                    single_pattern = [sam_output_name]
                    if external_protocol == 'sftp':
                        upload_files_to_external_sftp(
                            external_host, external_port, external_user, external_pass, external_dir,
                            tmp_dir, trace_id, job_id, single_pattern, metrics, transfer_status,
                            checksum_status, errors, warnings, DRY_RUN
                        )
                    else:
                        upload_files_to_external_ftp(
                            external_host, external_user, external_pass, external_dir,
                            tmp_dir, trace_id, job_id, single_pattern, metrics, transfer_status,
                            checksum_status, errors, warnings, DRY_RUN
                        )
                else:
                    logger.info(f"[{trace_id}] Final delivery skipped (SAM not copied back this run).")

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
        'body': json.dumps({'message': 'Pipeline completed.', 'trace_id': trace_id})
    }
