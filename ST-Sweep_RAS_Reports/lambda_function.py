import os
import json
import boto3
import paramiko
import tempfile
import shutil
import logging
import socket
import base64
import time
import stat
from datetime import timedelta
from typing import List, Optional
from boxsdk import JWTAuth, Client

from logging_utils import (
    log_job_start, log_job_end, log_sftp_connection, log_matched_files,
    log_checksum_ok, log_checksum_fail, log_file_transferred, log_archive,
    log_tmp_usage, log_warning, log_error, log_dry_run_action
)
from checksum_utils import log_checksum
from trace_utils import get_or_create_trace_id
from file_match_utils import match_files
from retry_utils import default_retry
from storage_utils import (
    get_prev_business_day, get_current_day, get_current_day_label, get_current_year_reports_folder_name,
    expand_patterns_for_date, ensure_box_path_and_upload
)
from performance_utils import time_operation
from alert_utils import send_file_transfer_sns_alert
from dry_run_utils import is_dry_run_enabled

logger = logging.getLogger()
logger.setLevel(logging.INFO)
# Tame Box SDK wire logs (409 conflicts are expected when versioning):
logging.getLogger("boxsdk").setLevel(logging.ERROR)

s3_client = boto3.client('s3')

# =============================
# Job configuration (defaults)
# =============================

# Job 1: /lti/PROD/<PrevBizDay>/
JOB1_BASE_DIR_DEFAULT = "/lti/PROD"
JOB1_REMOTE_DATE_FMT_ENV = "JOB1_REMOTE_DATE_FMT"  # default %Y%m%d
JOB1_REMOTE_DATE_FMT_DEFAULT = "%Y%m%d"

# By default, grab only DLYINVUB.TXT for JOB1 (exact filename match)
# You can override via env JOB1_PATTERNS (comma-separated). Examples:
#  - "DLYINVUB.TXT,DLYINVUB.PDF" (restrict to two files)
#  - "*" (match all regular files in the dated folder)
JOB1_PATTERNS_DEFAULT: List[str] = ["DLYINVUB.TXT"]

# Job 2: /lti/PROD/RAS/ (no date subfolder; filenames contain date)
JOB2_BASE_DIR_DEFAULT = "/lti/PROD/RAS"

# Patterns that render to current day (supports {YYYY-MM-DD}, etc.)
JOB2_PATTERNS_DEFAULT: List[str] = [
    "NPP_Transaction_History_Report_*_{YYYY-MM-DD}.xlsx",
    "Trial Balance Report*.zip",
]

# =============================
# Utilities
# =============================

def get_secret(secret_name):
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

def _val(d, *keys, default=None):
    """Fetch first present key (case-tolerant) from dict `d`."""
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    low = {str(k).lower(): v for k, v in d.items()}
    for k in keys:
        lk = str(k).lower()
        if lk in low and low[lk] not in (None, ""):
            return low[lk]
    return default

def get_effective_sftp_host(secret_host: str) -> str:
    """Prefer explicit override to route via VPC private IP, else env SFTP_HOST, else secret host."""
    host_override = os.getenv("SFTP_PRIVATE_IP") or os.getenv("SFTP_HOST")
    return host_override.strip() if host_override else secret_host

def preflight_network_check(host: str, port: int, timeout: float, trace_id: str) -> bool:
    logger.info("[%s] [NET] Preflight for %s:%s (timeout=%.1fs)", trace_id, host, port, timeout)
    try:
        infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
        addrs = list({(ai[4][0], ai[4][1]) for ai in infos})
        logger.info("[%s] [NET] DNS resolved: %s", trace_id, addrs)
    except Exception as e:
        log_error(trace_id, f"[NET] DNS resolution failed for {host}: {e}")
        return False

    ok = False
    for ip, p in addrs:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        start = time.time()
        try:
            s.connect((ip, p))
            elapsed = (time.time() - start) * 1000
            logger.info("[%s] [NET] TCP connect OK to %s:%s in %.1f ms", trace_id, ip, p, elapsed)
            ok = True
        except Exception as e:
            logger.error("[%s] [NET] TCP connect FAILED to %s:%s -> %s", trace_id, ip, p, e)
        finally:
            s.close()
    return ok

CONNECT_TIMEOUT = float(os.getenv("CONNECT_TIMEOUT", "6"))
BANNER_TIMEOUT  = float(os.getenv("BANNER_TIMEOUT",  "10"))
AUTH_TIMEOUT    = float(os.getenv("AUTH_TIMEOUT",    "10"))
SOCKET_TIMEOUT  = int(os.getenv("SOCKET_TIMEOUT",    "10"))
PREFLIGHT_TIMEOUT = float(os.getenv("PREFLIGHT_TIMEOUT", "3"))

@default_retry()
def create_sftp_client(host: str, port: int, username: str, password: str, hostkey_b64: Optional[str] = None):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        hostname=host,
        port=int(port),
        username=username,
        password=password,
        look_for_keys=False,
        allow_agent=False,
        timeout=CONNECT_TIMEOUT,
        banner_timeout=BANNER_TIMEOUT,
        auth_timeout=AUTH_TIMEOUT,
    )
    transport = client.get_transport()
    transport.set_keepalive(10)
    if hostkey_b64:
        got = base64.b64encode(transport.get_remote_server_key().asbytes()).decode()
        if got != hostkey_b64:
            client.close()
            raise paramiko.SSHException("Host key pin failed")
    return client.open_sftp()

# Small health check helper
def sftp_healthcheck(sftp) -> bool:
    try:
        sftp.listdir('.')
        return True
    except Exception:
        return False

# =============================
# Core sub-job runner
# =============================

def run_subjob(
    trace_id: str,
    sftp,
    box_client,
    s3_bucket: str,
    s3_prefix: str,
    s3_kms_key_arn: Optional[str],
    remote_base_dir: str,
    remote_date_fmt: Optional[str],
    patterns: List[str],
    use_prev_biz_date_in_filenames: bool,
    prev_biz_date,
    dry_run_enabled: bool,
    warnings: List[str],
    errors: List[str],
    job_name: str,
):
    """
    Execute one sub-job: list, filter, download, upload to S3 and Box with desired destination pathing.
    - remote_base_dir: e.g., "/lti/PROD" or "/lti/PROD/RAS"
    - remote_date_fmt: if provided, we append "<prevBizDay.strftime(fmt)>" as a subfolder under remote_base_dir; else base is used as-is
    - patterns: file patterns (exact names or tokenized with {YYYYMMDD}, {YYYY-MM-DD}, etc.)
    - use_prev_biz_date_in_filenames: if True, expand tokens using prev_biz_date for matching
    """
    logger.info("[%s] --- Subjob %s start ---", trace_id, job_name)

    # Resolve remote directory (Job1 uses prev biz day subfolder; Job2 stays flat by default)
    if remote_date_fmt:
        remote_dir = f"{remote_base_dir.rstrip('/')}/{prev_biz_date.strftime(remote_date_fmt)}"
    else:
        remote_dir = remote_base_dir

    # Expand tokenized patterns for *filename* date (Job2) or pass-through (Job1 exact list)
    effective_patterns = expand_patterns_for_date(
        patterns, prev_biz_date if use_prev_biz_date_in_filenames else None
    )

    logger.info("[%s] Subjob %s remote_dir=%s patterns=%s", trace_id, job_name, remote_dir, effective_patterns)

    # List and match with holiday fallback for dated dirs
    try:
        entries = sftp.listdir_attr(remote_dir)
    except Exception as e:
        msg = str(e).lower()
        if remote_date_fmt and ("no such file" in msg or getattr(e, 'errno', None) == 2):
            # Walk back up to 5 calendar days to find last existing folder
            fallback_found = False
            for i in range(1, 6):
                cand = prev_biz_date - timedelta(days=i)
                fallback_dir = f"{remote_base_dir.rstrip('/')}/{cand.strftime(remote_date_fmt)}"
                try:
                    entries = sftp.listdir_attr(fallback_dir)
                    log_warning(trace_id, f"{job_name}: {remote_dir} missing; using fallback {fallback_dir}")
                    remote_dir = fallback_dir
                    fallback_found = True
                    break
                except Exception:
                    continue
            if not fallback_found:
                errors.append(f"{job_name}: SFTP directory listing failed: {e}")
                log_error(trace_id, errors[-1])
                return [], []
        else:
            errors.append(f"{job_name}: SFTP directory listing failed: {e}")
            log_error(trace_id, errors[-1])
            return [], []

    # Only consider regular files; ignore directories to avoid download errors
    all_files = [e.filename for e in entries if stat.S_ISREG(e.st_mode)]
    skipped_dirs = [e.filename for e in entries if stat.S_ISDIR(e.st_mode)]
    if skipped_dirs:
        logger.info("[%s] %s: ignoring %d directories: %s", trace_id, job_name, len(skipped_dirs), skipped_dirs)
    logger.info("[%s] %s: listdir_attr returned %d regular files", trace_id, job_name, len(all_files))

    # Support match-all mode when patterns == ["*"] or empty
    match_all = False
    if not effective_patterns:
        match_all = True
    else:
        match_all = any(p.strip() == "*" for p in effective_patterns)

    if match_all:
        matched_files = sorted(all_files)
        unmatched = set()
        logger.info("[%s] %s: match-all mode enabled; selecting all %d files", trace_id, job_name, len(matched_files))
    else:
        matched_files = match_files(all_files, include_patterns=effective_patterns)
        unmatched = set(all_files) - set(matched_files)

    log_matched_files(trace_id, matched_files, unmatched)

    # Prepare destination folder labels (CURRENT day)
    year_folder = get_current_year_reports_folder_name()  # e.g., 2025Reports
    day_folder = get_current_day_label()                  # e.g., 20251014

    downloaded_files = []
    box_files = []

    with tempfile.TemporaryDirectory() as tmp_dir:
        free_mb = shutil.disk_usage(tmp_dir).free // (1024 * 1024)
        log_tmp_usage(trace_id, len(os.listdir(tmp_dir)), free_mb)

        for filename in matched_files:
            try:
                remote_path = f"{remote_dir.rstrip('/')}/{filename}"
                local_path = os.path.join(tmp_dir, filename)

                # Download
                _, _ = time_operation(sftp.get, remote_path, local_path)

                # Checksums
                downloaded_checksum = log_checksum(local_path, trace_id, algo="sha256", note=f"{job_name}: after download")
                s3_upload_checksum = log_checksum(local_path, trace_id, algo="sha256", note=f"{job_name}: before S3")

                if downloaded_checksum != s3_upload_checksum:
                    msg = f"{job_name}: Checksum mismatch for {filename}: {downloaded_checksum} != {s3_upload_checksum}"
                    errors.append(msg)
                    log_checksum_fail(trace_id, filename, downloaded_checksum, s3_upload_checksum)
                else:
                    log_checksum_ok(trace_id, filename, downloaded_checksum)

                # S3 upload: <prefix>/<YYYY>Reports/<YYYYMMDD>/<file>
                s3_key = f"{s3_prefix.rstrip('/')}/{year_folder}/{day_folder}/{filename}"
                extra_args = {"ServerSideEncryption": "aws:kms"}
                if s3_kms_key_arn:
                    extra_args["SSEKMSKeyId"] = s3_kms_key_arn

                if not dry_run_enabled:
                    _, s3_duration = time_operation(
                        s3_client.upload_file, local_path, s3_bucket, s3_key, ExtraArgs=extra_args
                    )
                    log_file_transferred(trace_id, filename, f"S3:{job_name}", s3_duration)
                    log_archive(trace_id, filename, s3_key)
                else:
                    log_dry_run_action(trace_id, f"{job_name}: Would upload {filename} to S3 at {s3_key}")

                downloaded_files.append(filename)
            except Exception as e:
                errors.append(f"{job_name}: File transfer failed for {filename}: {e}")
                log_error(trace_id, errors[-1])

        # Prepare Box upload list (just the files we actually downloaded)
        box_files = downloaded_files[:]
        if box_files:
            box_tmp_dir = os.path.join(tmp_dir, f"boxonly_{job_name}")
            os.makedirs(box_tmp_dir, exist_ok=True)
            for fname in box_files:
                shutil.copy2(os.path.join(tmp_dir, fname), os.path.join(box_tmp_dir, fname))

            if dry_run_enabled:
                log_dry_run_action(trace_id, f"{job_name}: Would upload files {box_files} to Box path {year_folder}/{day_folder}")
            else:
                try:
                    ensure_box_path_and_upload(
                        client=box_client,
                        root_folder_id=os.getenv('BOX_FOLDER_ID'),
                        path_components=[year_folder, day_folder],
                        local_dir=box_tmp_dir,
                        trace_context=trace_id
                    )
                except Exception as e:
                    errors.append(f"{job_name}: Box upload failed: {e}")
                    log_error(trace_id, errors[-1])
        else:
            warn = f"{job_name}: No files matched patterns for Box/S3"
            warnings.append(warn)
            log_warning(trace_id, warn)

    logger.info("[%s] --- Subjob %s end ---", trace_id, job_name)
    return downloaded_files, box_files


# =============================
# Lambda handler
# =============================

def lambda_handler(event, context):
    trace_id = get_or_create_trace_id(context)
    dry_run_enabled = is_dry_run_enabled()
    inject_error = os.getenv('INJECT_ERROR', 'false').lower() == 'true'
    socket.setdefaulttimeout(SOCKET_TIMEOUT)

    # Invocation context
    if isinstance(event, dict) and event.get("source") == "aws.events":
        logger.info("[%s] Invoked by EventBridge rule (detail-type=%s)", trace_id, event.get("detail-type"))
    else:
        logger.info("[%s] Invoked manually or by another source", trace_id)

    logger.info("[%s] Function=%s Version=%s ARN=%s", trace_id, context.function_name, context.function_version, context.invoked_function_arn)

    errors: List[str] = []
    warnings: List[str] = []
    all_s3_files: List[str] = []
    all_box_files: List[str] = []

    log_job_start(trace_id, trace_id, ["<see per-subjob config>"])

    try:
        # ---- Config & Secrets ----
        src_secret_name = os.getenv('SRC_SECRET_NAME') or (_ for _ in ()).throw(RuntimeError("SRC_SECRET_NAME env var is required"))
        box_secret_name = os.getenv('BOX_SECRET_NAME') or (_ for _ in ()).throw(RuntimeError("BOX_SECRET_NAME env var is required"))
        box_folder_id   = os.getenv('BOX_FOLDER_ID')   or (_ for _ in ()).throw(RuntimeError("BOX_FOLDER_ID env var is required"))

        s3_bucket = os.getenv('S3_BUCKET', 'jams-ftp-process-bucket')
        s3_prefix = os.getenv('S3_PREFIX', 'ftp-listings')
        s3_kms_key_arn = os.getenv('S3_KMS_KEY_ARN')

        # Resolve SFTP host/creds
        src_secret = get_secret(src_secret_name)
        secret_host = _val(src_secret, 'Host', 'host')
        src_host = get_effective_sftp_host(secret_host)
        src_user = _val(src_secret, 'Username', 'username')
        src_pass = _val(src_secret, 'Password', 'password')
        src_port = int(_val(src_secret, 'Port', 'port', default=22))

        if not (src_host and src_user and src_pass):
            raise RuntimeError(f"Missing required SFTP fields in secret {src_secret_name} (Host/Username/Password).")

        # Box client
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

        # Preflight network check
        if not preflight_network_check(src_host, src_port, timeout=PREFLIGHT_TIMEOUT, trace_id=trace_id):
            msg = f"Network preflight failed to {src_host}:{src_port} – check VPC/NAT/SG/NACL"
            errors.append(msg); log_error(trace_id, msg); raise RuntimeError(msg)

        # Connect SFTP
        try:
            sftp = create_sftp_client(
                src_host, src_port, src_user, src_pass, hostkey_b64=os.getenv("SSH_SERVER_KEY_B64")
            )
            log_sftp_connection(trace_id, src_host, "OPENED")
        except Exception as e:
            errors.append(f"SFTP connection failed: {e}")
            log_error(trace_id, errors[-1])
            raise

        # Determine previous business day (source) and prepare job configs
        prev_biz_date = get_prev_business_day()
        current_day_date = get_current_day()          # for Job 2 pattern expansion

        # JOB 1 config (date subfolder under /lti/PROD)
        job1_base_dir = os.getenv("JOB1_BASE_DIR", JOB1_BASE_DIR_DEFAULT)
        job1_remote_fmt = os.getenv(JOB1_REMOTE_DATE_FMT_ENV, JOB1_REMOTE_DATE_FMT_DEFAULT)
        # Allow optional override of list via env JOB1_PATTERNS (comma-separated). "*" means ALL files.
        job1_patterns_env = os.getenv("JOB1_PATTERNS", "").strip()
        job1_patterns = [p.strip() for p in job1_patterns_env.split(',') if p.strip()] or JOB1_PATTERNS_DEFAULT

        # JOB 2 config (flat dir, filenames contain date)
        job2_base_dir = os.getenv("JOB2_BASE_DIR", JOB2_BASE_DIR_DEFAULT)
        job2_remote_fmt = os.getenv("JOB2_REMOTE_DATE_FMT", "").strip() or None
        job2_patterns = [p.strip() for p in os.getenv("JOB2_PATTERNS", "").split(',') if p.strip()] or JOB2_PATTERNS_DEFAULT
        job2_patterns_effective = expand_patterns_for_date(job2_patterns, current_day_date)

        # ---- Run subjobs ----
        with sftp:  # guarantees close when block exits
            # JOB 1
            s3_1, box_1 = run_subjob(
                trace_id, sftp, box_client, s3_bucket, s3_prefix, s3_kms_key_arn,
                remote_base_dir=job1_base_dir,
                remote_date_fmt=job1_remote_fmt,  # append prevBizDay folder
                patterns=job1_patterns,           # exact list; default is only DLYINVUB.TXT
                use_prev_biz_date_in_filenames=False,
                prev_biz_date=prev_biz_date,
                dry_run_enabled=dry_run_enabled, warnings=warnings, errors=errors, job_name="JOB1_LTI_PROD"
            )

            # Ensure SFTP still alive before JOB 2; reconnect if needed
            if not sftp_healthcheck(sftp):
                try:
                    sftp.close()
                except Exception:
                    pass
                sftp = create_sftp_client(
                    src_host, src_port, src_user, src_pass, hostkey_b64=os.getenv("SSH_SERVER_KEY_B64")
                )
                log_sftp_connection(trace_id, src_host, "REOPENED")

            # JOB 2
            s3_2, box_2 = run_subjob(
                trace_id, sftp, box_client, s3_bucket, s3_prefix, s3_kms_key_arn,
                remote_base_dir=job2_base_dir,
                remote_date_fmt=job2_remote_fmt,      # none by default
                patterns=job2_patterns_effective,     # TODAY already rendered here
                use_prev_biz_date_in_filenames=False,
                prev_biz_date=prev_biz_date,
                dry_run_enabled=dry_run_enabled, warnings=warnings, errors=errors, job_name="JOB2_RAS"
            )

            all_s3_files.extend(s3_1 + s3_2)
            all_box_files.extend(box_1 + box_2)

        log_sftp_connection(trace_id, src_host, "CLOSED")

        if inject_error:
            raise RuntimeError("Injected test error for alerting")

    except Exception as e:
        errors.append(f"Unhandled exception: {e}")
        log_error(trace_id, errors[-1])

    # ---- Alerting ----
    if errors or warnings:
        send_file_transfer_sns_alert(
            trace_id=trace_id,
            s3_files=all_s3_files,
            box_files=all_box_files,
            checksum_results=None,
            errors=errors if errors else None,
            warnings=warnings if warnings else None,
            function_name=context.function_name,
            dry_run_enabled=dry_run_enabled,
            transfer_status="FAILURE" if errors else "WARNING"
        )
    else:
        logger.info(f"[{trace_id}] Both subjobs completed successfully with no errors or warnings.")

    log_job_end(trace_id, trace_id)

    return {
        'statusCode': 500 if errors else 200,
        'body': json.dumps({
            'message': "Errors occurred." if errors else "Both subjobs completed successfully.",
            'trace_id': trace_id,
            's3_files': all_s3_files,
            'box_files': all_box_files
        })
    }
    