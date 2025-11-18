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
    upload_files_to_box_prev_bday,   # kept for other jobs
    upload_sam_output_to_box,        # SAM -> yyyy/MM/yyyyMMdd
    upload_pe_output_to_box          # PE  -> yyyy/yyyyMMdd
)
from performance_utils import time_operation
from metrics_utils import publish_file_transfer_metric, publish_error_metric
from alert_utils import send_file_transfer_sns_alert
from dry_run_utils import is_dry_run_enabled, log_dry_run_action
from email_utils import send_email_with_attachment, parse_recipients, send_email_via_smtp

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.getLogger("boxsdk").setLevel(logging.WARNING)

s3_client = boto3.client('s3')
glue = boto3.client('glue')

# -------------------- helpers --------------------

def get_secret(secret_name):
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

def get_file_patterns():
    """FILE_PATTERN supports token {YYYYMMDD}; DATE_MODE controls pick date."""
    val = os.getenv('FILE_PATTERN')
    patterns = [x.strip() for x in val.split(',')] if val else ['*']
    pick_mode = os.getenv('DATE_MODE', 'today').lower()
    pick_day_key = get_business_day_str(pick_mode)  # 'YYYYMMDD'
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

    store_mode = os.getenv('STORE_DATE_MODE', os.getenv('DATE_MODE', 'today')).lower()
    store_day_key = get_business_day_str(store_mode)

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

            s3_key = f"{prefix}/{store_day_key}/{filename}" if prefix else f"{store_day_key}/{filename}"

            if DRY_RUN:
                log_dry_run_action("Would upload to S3", local_path, bucket, s3_key)
            else:
                _, s3_duration = time_operation(s3_client.upload_file, local_path, bucket, s3_key)
                log_file_transferred(trace_id, filename, "S3", s3_duration)
                log_archive(trace_id, filename, s3_key)

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

    for d, b in (("S3", mb), ("SFTP", mb)):
        metrics[f"{d} total mb"] = f"{b:.2f}"
        metrics[f"{d} upload speed mb/s" if d == "S3" else f"{d} download speed mb/s"] = f"{mbps:.2f}"

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

# ---------- SFTP helpers ----------
def _sftp_mkdirs_and_cd_portable(sftp, path: str | None, *, create: bool = True):
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
            _sftp_mkdirs_and_cd_portable(sftp, remote_dir, create=os.getenv("EXT_CREATE_REMOTE_PATH","true").lower()=="true")
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

# ---------- Local -> External FTP (kept for completeness) ----------
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
            ftp.cwd('/')
            for part in parts:
                if os.getenv("EXT_CREATE_REMOTE_PATH","true").lower()=="true":
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

# ---------- Glue helpers ----------
def start_and_wait_glue(job_name, arguments, poll_sec=15, timeout_sec=1800, trace_id=""):
    """Start a Glue job and wait for completion; return final state."""
    try:
        run_id = glue.start_job_run(JobName=job_name, Arguments=arguments)["JobRunId"]
        logger.info(f"[{trace_id}] Glue job {job_name} started: {run_id}")
    except Exception as e:
        raise RuntimeError(f"Failed to start Glue job {job_name}: {e}")

    deadline = time.time() + timeout_sec
    state = "STARTING"
    while time.time() < deadline:
        try:
            resp = glue.get_job_run(JobName=job_name, RunId=run_id)
            state = resp["JobRun"]["JobRunState"]
            if state in ("SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT"):
                logger.info(f"[{trace_id}] Glue job {job_name} finished with state={state}")
                return state
        except Exception as e:
            logger.warning(f"[{trace_id}] get_job_run error for {job_name}/{run_id}: {e}")
        time.sleep(poll_sec)
    logger.warning(f"[{trace_id}] Glue job {job_name} timed out after {timeout_sec}s")
    return "TIMEOUT"

# ---------- Handler ----------
def lambda_handler(event, context):
    trace_id = get_or_create_trace_id(context)
    job_id = trace_id
    file_patterns = get_file_patterns()

    errors, warnings, metrics = [], [], {}
    transfer_status, checksum_status = {}, {}

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
        box_folder_id    = os.getenv('BOX_FOLDER_ID')

        s3_bucket        = os.getenv('S3_BUCKET', 'ninepoint-files-prod')
        s3_prefix        = os.getenv('S3_PREFIX', 'ST-Transfer-NPP-Aladdin-Capstock')
        function_name    = os.getenv('AWS_LAMBDA_FUNCTION_NAME', 'N/A')

        # Outputs (S3)
        out_s3_bucket    = os.getenv('OUT_S3_BUCKET', s3_bucket)
        out_s3_prefix    = os.getenv('OUT_S3_PREFIX', 'capstock/outputs')
        pe_output_name   = os.getenv('PE_OUTPUT_NAME', 'PE Sub Red.csv')
        sam_output_name  = os.getenv('SAM_OUTPUT_NAME', 'SAM Sub Red.csv')

        # Email env
        email_transport  = os.getenv("EMAIL_TRANSPORT", "ses").lower()  # 'smtp' or 'ses'
        ses_region       = os.getenv("SES_REGION", "us-east-1")
        ses_sender       = os.getenv("SES_SENDER", "notifications@ninepoint.com")
        # SAM recipients
        sam_email_to_raw = os.getenv("SAM_EMAIL_TO", "fundops@ninepoint.com")
        sam_email_to     = parse_recipients(sam_email_to_raw)

        # PE recipients – default to the full list
        pe_email_default = ",".join([
            "fundops@ninepoint.com",
            "operations@peinvestments.com",
            "csalsman@peinvestments.com",
            "ahartshorn@peinvestments.com",
            "statements@peinvestments.com",
        ])
        pe_email_to_raw = os.getenv("PE_EMAIL_TO", pe_email_default)
        pe_email_to     = parse_recipients(pe_email_to_raw)
        sam_subj        = os.getenv("SAM_EMAIL_SUBJECT", "Ninepoint Partners - DATA VALIDATION FOR SPROTT")
        sam_body_plain   = os.getenv("SAM_EMAIL_BODY_PLAIN",
                            "Hello,\n\nPlease find attached Subscriptions and Redemptions report.\n\n"
                            "If you are experiencing a support issue or have any other needs for this report, please reply all to this email.\n\nThanks")
        sam_body_html    = os.getenv("SAM_EMAIL_BODY_HTML", None)

        pe_subj          = os.getenv("PE_EMAIL_SUBJECT", "Ninepoint Partners - Subscriptions and Redemptions for PE")
        pe_body_plain    = os.getenv("PE_EMAIL_BODY_PLAIN",
                            "Hello,\n\nPlease find attached PE Subscriptions and Redemptions report.\n\n"
                            "If you are experiencing a support issue or have any other needs for this report, please reply all to this email.\n\nThanks")
        pe_body_html     = os.getenv("PE_EMAIL_BODY_HTML", None)

        # SMTP details (if EMAIL_TRANSPORT=smtp)
        smtp_host        = os.getenv("SMTP_HOST", "10.0.31.212")
        smtp_port        = int(os.getenv("SMTP_PORT", "25"))
        smtp_from        = os.getenv("SMTP_FROM", ses_sender)
        smtp_starttls    = os.getenv("SMTP_STARTTLS", "false").lower() == "true"
        smtp_user        = os.getenv("SMTP_USERNAME")
        smtp_pass        = os.getenv("SMTP_PASSWORD")
        smtp_helo        = os.getenv("SMTP_HELO")
        smtp_dsn_notify  = os.getenv("SMTP_DSN_NOTIFY", "SUCCESS,FAILURE")
        smtp_dsn_ret     = os.getenv("SMTP_DSN_RET", "HDRS")

        # Glue jobs
        glue_pe_job      = os.getenv("GLUE_PE_JOB", "pe-capstock-glue")
        glue_sam_job     = os.getenv("GLUE_SAM_JOB", "sprott-capstock-glue")
        glue_poll        = int(os.getenv("GLUE_POLL_INTERVAL_SEC", "15"))
        glue_timeout     = int(os.getenv("GLUE_TIMEOUT_SEC", "1800"))

        # Source SFTP
        src_secret = get_secret(src_secret_name)
        src_host = src_secret['Host']
        src_user = src_secret['Username']
        src_pass = src_secret['Password']
        src_port = int(src_secret.get('Port') or 22)
        src_dir  = os.getenv('SRC_REMOTE_DIR', '.')

        # External Sprott target
        ext_secret = get_secret(ext_secret_name)
        external_host = ext_secret.get('host') or ext_secret.get('Host')
        external_user = ext_secret['Username']
        external_pass = ext_secret.get('password') or ext_secret.get('Password')
        external_port = int(ext_secret.get('port') or ext_secret.get('Port') or 21)
        external_dir  = os.getenv('EXT_REMOTE_DIR', '.')
        auto_proto    = 'sftp' if external_port == 22 or str(external_host).lower().startswith('sftp.') else 'ftp'
        external_protocol = os.getenv('EXT_PROTOCOL', auto_proto).lower()
        external_host_for_alert = external_host or external_host_for_alert

        # Box client
        box_client = None
        box_sam_folder_id = os.getenv('BOX_SAM_FOLDER_ID')
        box_pe_folder_id  = os.getenv('BOX_PE_FOLDER_ID')
        if box_secret_name and (box_folder_id or box_sam_folder_id or box_pe_folder_id):
            box_cfg = get_secret(box_secret_name)
            auth = JWTAuth(
                client_id=box_cfg['boxAppSettings']['clientID'],
                client_secret=box_cfg['boxAppSettings']['clientSecret'],
                enterprise_id=box_cfg['enterpriseID'],
                jwt_key_id=box_cfg['boxAppSettings']['appAuth']['publicKeyID'],
                rsa_private_key_data=box_cfg['boxAppSettings']['appAuth']['privateKey'],
                rsa_private_key_passphrase=box_cfg['boxAppSettings']['appAuth']['passphrase'].encode('utf-8'),
            )
            box_client = Client(auth)

        # Dates
        store_mode = os.getenv('STORE_DATE_MODE', os.getenv('DATE_MODE', 'today')).lower()
        store_day_key = get_business_day_str(store_mode)
        output_day_mode = os.getenv('OUTPUT_DATE_MODE', 'today').lower()
        output_day_key  = get_business_day_str(output_day_mode)
        logger.info(f"[{trace_id}] Output day key (PE/SAM): {output_day_key}")

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

            # 1) SFTP -> S3 (dated)
            download_and_upload_to_s3(
                src_sftp, src_dir, s3_bucket, s3_prefix, tmp_dir, trace_id, job_id,
                file_patterns, metrics, transfer_status, checksum_status, errors, warnings, DRY_RUN
            )
            src_sftp.close()
            log_sftp_connection(trace_id, src_host, "CLOSED")

            free_mb = shutil.disk_usage(tmp_dir).free // (1024 * 1024)
            log_tmp_usage(trace_id, len(os.listdir(tmp_dir)), free_mb)

            # Determine the input file name (first matched)
            files_for_glue = match_files(os.listdir(tmp_dir), include_patterns=file_patterns)
            if not files_for_glue:
                msg = "No source files were transferred; skipping Glue, emails, Box upload, and final delivery."
                logger.info(f"[{trace_id}] {msg}")
                warnings.append(msg)
                transfer_status["pipeline"] = "SKIPPED_NO_SOURCE"
            else:
                capstock_name = files_for_glue[0]
                s3_src_key = f"{s3_prefix}/{store_day_key}/{capstock_name}"
                input_s3_uri = f"s3://{s3_bucket}/{s3_src_key}"

                # 2) Trigger Glue jobs and wait
                glue_args = {
                    "--INPUT_S3_URI":  input_s3_uri,
                    "--OUTPUT_BUCKET": out_s3_bucket,
                    "--OUTPUT_PREFIX": out_s3_prefix,
                    "--AS_OF":         output_day_key
                }

                if DRY_RUN:
                    log_dry_run_action("Would start Glue job", glue_pe_job, glue_args)
                    log_dry_run_action("Would start Glue job", glue_sam_job, glue_args)
                    glue_states = {pe_output_name: "SUCCEEDED", sam_output_name: "SUCCEEDED"}
                else:
                    st_pe  = start_and_wait_glue(glue_pe_job, glue_args, glue_poll, glue_timeout, trace_id)
                    st_sam = start_and_wait_glue(glue_sam_job, glue_args, glue_poll, glue_timeout, trace_id)
                    glue_states = {pe_output_name: st_pe, sam_output_name: st_sam}

                # 3) Prepare for email / Box only if Glue succeeded
                copied = {
                    pe_output_name:  glue_states.get(pe_output_name)  == "SUCCEEDED",
                    sam_output_name: glue_states.get(sam_output_name) == "SUCCEEDED",
                }

                # 4) Email the two outputs (only if they exist this run)
                try:
                    if os.getenv("ENABLE_EMAIL", "true").lower() == "true":
                        pe_key  = f"{out_s3_prefix}/{output_day_key}/{pe_output_name}"
                        sam_key = f"{out_s3_prefix}/{output_day_key}/{sam_output_name}"
                        pe_path  = os.path.join(tmp_dir, pe_output_name)
                        sam_path = os.path.join(tmp_dir, sam_output_name)

                        if not DRY_RUN:
                            if copied.get(pe_output_name, False):
                                s3_client.download_file(out_s3_bucket, pe_key, pe_path)
                            if copied.get(sam_output_name, False):
                                s3_client.download_file(out_s3_bucket, sam_key, sam_path)

                        if copied.get(sam_output_name, False):
                            if DRY_RUN:
                                log_dry_run_action("Would email SAM report",
                                                   (smtp_from if email_transport=="smtp" else ses_sender), sam_email_to)
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
                            logger.info(f"[{trace_id}] SAM email suppressed (not produced this run).")

                        if copied.get(pe_output_name, False):
                            if DRY_RUN:
                                log_dry_run_action("Would email PE report",
                                                   (smtp_from if email_transport=="smtp" else ses_sender), pe_email_to)
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
                            logger.info(f"[{trace_id}] PE email suppressed (not produced this run).")

                except Exception as e:
                    err = f"Email delivery failed: {e}"
                    log_error(trace_id, err)
                    errors.append(err)

                # 5) Box upload of originals (store_day_key)
                if box_client and box_folder_id:
                    box_files = match_files(os.listdir(tmp_dir), include_patterns=file_patterns)
                    unmatched = set(os.listdir(tmp_dir)) - set(box_files)
                    if not box_files:
                        warn = "No files matched FILE_PATTERN for Box originals; skipping."
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
                            log_file_transferred(trace_id, f"{len(box_files)} file(s)", "Box", t1 - t0)
                            for fname in box_files:
                                log_box_version(trace_id, fname, "box_id", "box_version")
                            transfer_status["box"] = f"SUCCESS ({', '.join(box_files)})"
                        elif DRY_RUN:
                            log_dry_run_action("Would upload files to Box (day folder)", "tmp://boxonly", f"{box_folder_id}/{store_day_key}")
                            transfer_status["box"] = f"SUCCESS ({', '.join(box_files)})"
                    except Exception as e:
                        err = f"Box originals upload failed: {e}"
                        log_error(trace_id, err)
                        errors.append(err)
                        transfer_status["box"] = f"FAILED ({e})"

                # 6) Box upload of outputs (today)
                if box_client and (box_sam_folder_id or box_pe_folder_id):
                    if copied.get(sam_output_name, False):
                        if DRY_RUN:
                            log_dry_run_action("Would upload SAM to Box hierarchy yyyy/MM/yyyyMMdd",
                                               sam_output_name, box_sam_folder_id)
                        else:
                            try:
                                sam_path = os.path.join(tmp_dir, sam_output_name)
                                upload_sam_output_to_box(box_client, box_sam_folder_id, sam_path, output_day_key, context)
                                logger.info(f"[{trace_id}] SAM uploaded to Box output hierarchy.")
                            except Exception as e:
                                log_warning(trace_id, f"SAM Box upload (hierarchy) failed: {e}")
                    if copied.get(pe_output_name, False):
                        if DRY_RUN:
                            log_dry_run_action("Would upload PE to Box hierarchy yyyy/yyyyMMdd",
                                               pe_output_name, box_pe_folder_id)
                        else:
                            try:
                                pe_path = os.path.join(tmp_dir, pe_output_name)
                                upload_pe_output_to_box(box_client, box_pe_folder_id, pe_path, output_day_key, context)
                                logger.info(f"[{trace_id}] PE uploaded to Box output hierarchy.")
                            except Exception as e:
                                log_warning(trace_id, f"PE Box upload (hierarchy) failed: {e}")

                # 7) Final delivery: SAM to Sprott from S3
                if copied.get(sam_output_name, False):
                    sam_s3_key = f"{out_s3_prefix}/{output_day_key}/{sam_output_name}"
                    local_sam  = os.path.join(tmp_dir, sam_output_name)
                    if DRY_RUN:
                        open(local_sam, "wb").close()
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
                    logger.info(f"[{trace_id}] Final Sprott delivery skipped (SAM not produced this run).")

                free_mb = shutil.disk_usage(tmp_dir).free // (1024 * 1024)
                log_tmp_usage(trace_id, len(os.listdir(tmp_dir)), free_mb)

    except Exception as e:
        err = f"Unhandled exception: {e}"
        log_error(trace_id, err, exc=e)
        errors.append(err)
    finally:
        try:
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
        except Exception as ae:
            logger.error(f"[{trace_id}] ERROR: failed to publish SNS alert: {ae}")

    log_job_end(trace_id, job_id)

    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Pipeline completed.', 'trace_id': trace_id})
    }
