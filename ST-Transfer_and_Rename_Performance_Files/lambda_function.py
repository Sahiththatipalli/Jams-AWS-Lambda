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
    get_prev_business_day_label,
    upload_files_to_box_prev_bizday,  
    upload_files_to_box_two_locations      # NEW: daily + archive
)
from performance_utils import time_operation
from alert_utils import send_file_transfer_sns_alert
from dry_run_utils import is_dry_run_enabled

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.getLogger("boxsdk").setLevel(logging.WARNING)

s3_client = boto3.client('s3')

# ---------- Helpers ----------

def get_secret(secret_name):
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

def get_file_patterns():
    val = os.getenv('FILE_PATTERN')
    if val:
        return [x.strip() for x in val.split(',') if x.strip()]
    return ['*']

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
    """
    Allow override via env. If SFTP_PRIVATE_IP is set, prefer it (same-VPC EC2).
    Else use SFTP_HOST env, else the secret host.
    """
    host_override = os.getenv("SFTP_PRIVATE_IP") or os.getenv("SFTP_HOST")
    return host_override.strip() if host_override else secret_host

def preflight_network_check(host: str, port: int, timeout: float = 3.0, trace_id: str = "-") -> bool:
    """
    Logs DNS resolution and attempts raw TCP connects to each resolved IP.
    Returns True if any address connects, else False.
    """
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

# Make SFTP connect fail fast; let retry_utils control how many attempts.
CONNECT_TIMEOUT = float(os.getenv("CONNECT_TIMEOUT", "6"))
BANNER_TIMEOUT  = float(os.getenv("BANNER_TIMEOUT",  "10"))
AUTH_TIMEOUT    = float(os.getenv("AUTH_TIMEOUT",    "10"))
SOCKET_TIMEOUT  = int(os.getenv("SOCKET_TIMEOUT",    "10"))

@default_retry()  # uses your shared retry/backoff policy
def create_sftp_client(host: str, port: int, username: str, password: str, hostkey_b64: str | None = None):
    """
    Robust SFTP connect using SSHClient with explicit timeouts and keepalive.
    Optional host-key pinning via base64 key material.
    """
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

# ---------- Handler ----------

def lambda_handler(event, context):
    trace_id = get_or_create_trace_id(context)
    dry_run_enabled = is_dry_run_enabled()
    inject_error = os.getenv('INJECT_ERROR', 'false').lower() == 'true'

    # Bound all socket ops to avoid full-function hangs
    socket.setdefaulttimeout(SOCKET_TIMEOUT)

    # Tag if invoked by EventBridge for easier correlation
    invoked_by_eventbridge = isinstance(event, dict) and event.get("source") == "aws.events"
    if invoked_by_eventbridge:
        logger.info("[%s] Invoked by EventBridge rule (detail-type=%s)", trace_id, event.get("detail-type"))
    else:
        logger.info("[%s] Invoked manually or by another source", trace_id)

    # Helpful context logging
    logger.info("[%s] Function=%s Version=%s ARN=%s", trace_id, context.function_name, context.function_version, context.invoked_function_arn)

    errors = []
    warnings = []
    file_patterns = get_file_patterns()
    job_id = trace_id

    log_job_start(trace_id, job_id, file_patterns)

    checksum_status = {}
    box_files = []
    src_sftp = None

    try:
        # ---- Config & Secrets ----
        src_secret_name = os.getenv('SRC_SECRET_NAME')
        if not src_secret_name:
            raise RuntimeError("SRC_SECRET_NAME env var is required")

        box_secret_name = os.getenv('BOX_SECRET_NAME')
        if not box_secret_name:
            raise RuntimeError("BOX_SECRET_NAME env var is required")

        # Legacy single target (optional fallback)
        box_folder_id_legacy = os.getenv('BOX_FOLDER_ID')

        # New two-destination envs
        box_folder_id_daily = os.getenv('BOX_FOLDER_ID_DAILY')      # Daily Valuator Files (no date subfolder)
        box_folder_id_archive = os.getenv('BOX_FOLDER_ID_ARCHIVE')  # Archive root (we'll create <yyyyMMdd>)

        s3_bucket = os.getenv('S3_BUCKET', 'jams-ftp-process-bucket')
        s3_prefix = os.getenv('S3_PREFIX', 'ftp-listings')
        s3_kms_key_arn = os.getenv('S3_KMS_KEY_ARN')  # optional

        # Compute the single target folder name once (previous business day)
        date_folder = get_prev_business_day_label()
        logger.info("[%s] Using single date folder '%s' for both S3 and Box Archive", trace_id, date_folder)

        src_secret = get_secret(src_secret_name)
        secret_host = _val(src_secret, 'Host', 'host')
        src_host = get_effective_sftp_host(secret_host)
        src_user = _val(src_secret, 'Username', 'username')
        src_pass = _val(src_secret, 'Password', 'password')
        src_port = int(_val(src_secret, 'Port', 'port', default=22))
        src_dir  = os.getenv('SRC_REMOTE_DIR', '.')

        if not (src_host and src_user and src_pass):
            raise RuntimeError(f"Missing required SFTP fields in secret {src_secret_name} (Host/Username/Password).")

        # Emit the chosen host so we can detect alias/version/env drift
        logger.info("[%s] SFTP host (effective)=%s port=%s (secret host=%s)", trace_id, src_host, src_port, secret_host)

        # ---- Box config ----
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

        with tempfile.TemporaryDirectory() as tmp_dir:
            free_mb = shutil.disk_usage(tmp_dir).free // (1024 * 1024)
            if free_mb < 100:
                warning_msg = f"Low disk space: {free_mb} MB free"
                warnings.append(warning_msg)
                log_warning(trace_id, warning_msg)
            log_tmp_usage(trace_id, len(os.listdir(tmp_dir)), free_mb)

            # ---- Network preflight (DNS + TCP 22) ----
            preflight_timeout = float(os.getenv("PREFLIGHT_TIMEOUT", "3"))
            if not preflight_network_check(src_host, src_port, timeout=preflight_timeout, trace_id=trace_id):
                msg = f"Network preflight failed to {src_host}:{src_port} – check VPC/NAT/SG/NACL or alias/version target"
                errors.append(msg)
                log_error(trace_id, msg)
                raise RuntimeError(msg)

            # ---- Connect to SFTP ----
            try:
                src_sftp = create_sftp_client(
                    src_host, src_port, src_user, src_pass,
                    hostkey_b64=os.getenv("SSH_SERVER_KEY_B64")  # optional pin
                )
                log_sftp_connection(trace_id, src_host, "OPENED")
            except Exception as e:
                error_msg = f"SFTP connection failed: {e}"
                errors.append(error_msg)
                log_error(trace_id, error_msg)
                raise

            # ---- List remote ----
            try:
                logger.info("[%s] Running SFTP listdir_attr on %s", trace_id, src_dir)
                entries = src_sftp.listdir_attr(src_dir)
                all_files = [e.filename for e in entries]
                logger.info("[%s] listdir_attr returned %d items", trace_id, len(all_files))
            except Exception as e:
                error_msg = f"SFTP directory listing failed: {e}"
                errors.append(error_msg)
                log_error(trace_id, error_msg)
                raise

            matched_files = match_files(all_files, include_patterns=file_patterns)
            if not matched_files:
                warning_msg = "No files matched FILE_PATTERN for S3"
                warnings.append(warning_msg)
                log_warning(trace_id, warning_msg)

            unmatched = set(all_files) - set(matched_files)
            log_matched_files(trace_id, matched_files, unmatched)

            # ---- Download & S3 ----
            for filename in matched_files:
                try:
                    remote_path = f"{src_dir.rstrip('/')}/{filename}"
                    local_path = os.path.join(tmp_dir, filename)

                    # Download
                    _, duration = time_operation(src_sftp.get, remote_path, local_path)

                    # Checksums
                    downloaded_checksum = log_checksum(local_path, trace_id, algo="sha256", note="after SFTP download")
                    s3_upload_checksum = log_checksum(local_path, trace_id, algo="sha256", note="before S3 upload")

                    if downloaded_checksum != s3_upload_checksum:
                        error_msg = (f"Checksum mismatch for {filename}: "
                                     f"downloaded {downloaded_checksum} != s3 {s3_upload_checksum}")
                        errors.append(error_msg)
                        log_checksum_fail(trace_id, filename, downloaded_checksum, s3_upload_checksum)
                    else:
                        log_checksum_ok(trace_id, filename, downloaded_checksum)

                    checksum_status[filename] = "OK" if downloaded_checksum == s3_upload_checksum else "FAIL"

                    # S3 upload (single date folder + KMS)
                    s3_key = f"{s3_prefix.rstrip('/')}/{date_folder}/{filename}"
                    extra_args = {"ServerSideEncryption": "aws:kms"}
                    if s3_kms_key_arn:
                        extra_args["SSEKMSKeyId"] = s3_kms_key_arn

                    if not dry_run_enabled:
                        _, s3_duration = time_operation(
                            s3_client.upload_file,
                            local_path, s3_bucket, s3_key,
                            ExtraArgs=extra_args
                        )
                        log_file_transferred(trace_id, filename, "S3", s3_duration)
                        log_archive(trace_id, filename, s3_key)
                    else:
                        log_dry_run_action(trace_id, f"Would upload {filename} to S3 at {s3_key}")

                except Exception as e:
                    error_msg = f"File transfer failed for {filename}: {e}"
                    errors.append(error_msg)
                    log_error(trace_id, error_msg)

            # ---- Close SFTP ----
            try:
                if src_sftp:
                    src_sftp.close()
                log_sftp_connection(trace_id, src_host, "CLOSED")
            except Exception:
                pass

            # ---- Prepare for Box (two-location logic) ----
            box_files = match_files(os.listdir(tmp_dir), include_patterns=file_patterns)
            unmatched_box = set(os.listdir(tmp_dir)) - set(box_files)
            log_matched_files(trace_id, box_files, unmatched_box)

            if box_files:
                box_tmp_dir = os.path.join(tmp_dir, "boxonly")
                os.makedirs(box_tmp_dir, exist_ok=True)
                for fname in box_files:
                    shutil.copy2(os.path.join(tmp_dir, fname), os.path.join(box_tmp_dir, fname))

                if dry_run_enabled:
                    log_dry_run_action(trace_id, f"Would upload files {box_files} to Box destinations")
                else:
                    try:
                        if box_folder_id_daily and box_folder_id_archive:
                            # New behavior: upload to Daily (no date subfolder) + Archive/<yyyyMMdd>
                            upload_files_to_box_two_locations(
                                box_client,
                                daily_folder_id=box_folder_id_daily,
                                archive_root_folder_id=box_folder_id_archive,
                                local_dir=box_tmp_dir,
                                context=context
                            )
                        elif box_folder_id_legacy:
                            # Fallback: legacy single destination, uploads into <yyyyMMdd> subfolder
                            upload_files_to_box_prev_bizday(
                                box_client,
                                box_folder_id_legacy,
                                box_tmp_dir,
                                context
                            )
                        else:
                            raise RuntimeError(
                                "Missing Box folder env vars: set BOX_FOLDER_ID_DAILY and BOX_FOLDER_ID_ARCHIVE "
                                "(or BOX_FOLDER_ID for legacy single-destination)."
                            )
                    except Exception as e:
                        error_msg = f"Box upload failed: {e}"
                        errors.append(error_msg)
                        log_error(trace_id, error_msg)
            else:
                warning_msg = "No files matched FILE_PATTERN for Box"
                warnings.append(warning_msg)
                log_warning(trace_id, warning_msg)

            free_mb = shutil.disk_usage(tmp_dir).free // (1024 * 1024)
            log_tmp_usage(trace_id, len(os.listdir(tmp_dir)), free_mb)

            if inject_error:
                raise RuntimeError("Injected test error for alerting")

    except Exception as e:
        error_msg = f"Unhandled exception: {e}"
        errors.append(error_msg)
        log_error(trace_id, error_msg)

    # ---- Alerting ----
    if errors or warnings:
        send_file_transfer_sns_alert(
            trace_id=trace_id,
            s3_files=list(checksum_status.keys()),
            box_files=box_files,
            checksum_results=[{"file": k, "status": v} for k, v in checksum_status.items()],
            errors=errors if errors else None,
            warnings=warnings if warnings else None,
            function_name=context.function_name,
            dry_run_enabled=dry_run_enabled,
            transfer_status="FAILURE" if errors else "WARNING"
        )
    else:
        logger.info(f"[{trace_id}] File transfer completed successfully with no errors or warnings.")

    log_job_end(trace_id, job_id)

    status_code = 500 if errors else 200
    message = "Errors occurred during file transfer." if errors else "Files transferred successfully."

    return {
        'statusCode': status_code,
        'body': json.dumps({'message': message, 'trace_id': trace_id})
    }
