# lambda_function.py
# Runtime: Python 3.12
#
# Required env vars:
#   SRC_SECRET_NAME           = <SecretsManager name for SFTP (host/port/user + key or pwd)>
#   BOX_SECRET_NAME           = <SecretsManager name for Box JWT app>
#   BOX_FOLDER_ID             = <Box folder ID to upload into>
#   FILE_PATTERN              = %Y%m%d_CboeCanada*.csv   (supports strftime + {TODAY:..}/{PREV_BIZDAY:..})
#   SRC_REMOTE_DIR            = /transfer_out
#   S3_BUCKET                 = ninepoint-files-prod
#   S3_PREFIX                 = transfer-NEO-ETF-reports-to-box
#   TIMEZONE                  = America/Toronto          (default: UTC)
#   HOST_KEY_SHA256           = SHA256:Erka1+Sy/o9...    (pin after first successful run)
# Optional env vars:
#   SSH_SERVER_KEY_B64        = <exact base64 of server host key, alt to SHA256 pin>
#   S3_KMS_KEY_ARN            = <KMS key ARN for SSE-KMS> (if needed)
#   CONNECT_TIMEOUT/BANNER_TIMEOUT/AUTH_TIMEOUT/SOCKET_TIMEOUT/PREFLIGHT_TIMEOUT
#   INJECT_ERROR=true         (to test alerting)
#
# Secret JSON (SFTP) accepted fields (case-insensitive):
# {
#   "host": "files.cboe.com",
#   "port": 22,
#   "username": "dcovic@ninepoint.com",
#   "password": null,                         # if using password auth
#   "private_key_pem": "-----BEGIN OPENSSH PRIVATE KEY-----\\n...\\n-----END OPENSSH PRIVATE KEY-----\\n",
#   "private_key_b64": "<base64-of-key-file>",# alternative to private_key_pem
#   "passphrase": null                        # or "your-passphrase"
# }

import os
import re
import json
import boto3
import paramiko
import tempfile
import shutil
import logging
import socket
import base64
import time
import textwrap
import hashlib
from datetime import datetime
from zoneinfo import ZoneInfo
from boxsdk import JWTAuth, Client
from cryptography.hazmat.primitives import serialization

from logging_utils import (
    log_job_start, log_job_end, log_sftp_connection, log_matched_files,
    log_checksum_ok, log_checksum_fail, log_file_transferred, log_archive,
    log_tmp_usage, log_warning, log_error, log_dry_run_action
)
from storage_utils import (
    get_today_label,
    get_prev_business_day_label,
    get_yesterday_label,                     # NEW
    upload_files_to_box_today,
    upload_files_to_box_prev_bizday,
    upload_files_to_box_yesterday            # NEW
)
from checksum_utils import log_checksum
from trace_utils import get_or_create_trace_id
from file_match_utils import match_files
from retry_utils import default_retry
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

_TOKEN_RE = re.compile(r"\{(TODAY|YESTERDAY|PREV_BIZDAY)(?::([^}]+))?\}")

def _expand_date_tokens(s: str) -> str:
    """
    Replace {TODAY:%Y%m%d}, {YESTERDAY:%Y%m%d}, and {PREV_BIZDAY:%Y%m%d} tokens.
    If ANCHOR_DATE is set (defaults to YESTERDAY), {TODAY:...} is interpreted per anchor.
    """
    anchor = os.getenv("ANCHOR_DATE", "YESTERDAY").upper()

    def repl(m):
        token, fmt = m.group(1), (m.group(2) or "%Y%m%d")
        if token == "TODAY":
            if anchor == "YESTERDAY":
                return get_yesterday_label(fmt)
            elif anchor in ("PREV_BIZDAY", "PREVIOUS_BUSINESS_DAY", "PREV_BUSINESS_DAY"):
                return get_prev_business_day_label(fmt)
            else:
                return get_today_label(fmt)
        elif token == "YESTERDAY":
            return get_yesterday_label(fmt)
        elif token == "PREV_BIZDAY":
            return get_prev_business_day_label(fmt)

    return _TOKEN_RE.sub(repl, s)

def get_file_patterns():
    """
    Supports:
      - raw strftime tokens like:   %Y%m%d_CboeCanada*.csv
      - custom tokens like:         {TODAY:%Y%m%d}_CboeCanada*.csv
                                    {PREV_BIZDAY:%Y%m%d}_CboeCanada*.csv
    """
    raw = os.getenv('FILE_PATTERN')
    patterns = [x.strip() for x in raw.split(',')] if raw else ['*']

    # First expand custom tokens
    expanded = [_expand_date_tokens(p) for p in patterns if p]

    # Also expand plain strftime tokens (e.g., %Y%m%d)
    tz = os.getenv("TIMEZONE", "UTC")
    now = datetime.now(ZoneInfo(tz))
    out = []
    for p in expanded:
        if "%" in p:
            try:
                p = now.strftime(p)
            except Exception:
                pass
        out.append(p)
    return out

def _val(d, *keys, default=None):
    """Fetch first present key (case tolerant) from dict `d`."""
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

# --- Key helpers (robust to \n escaping and formats) ---

def _normalize_passphrase(pw):
    if pw is None: return None
    if isinstance(pw, str) and pw.strip().lower() in ("", "null", "none"):
        return None
    return pw

def _normalize_pem_text(pem: str) -> str:
    """
    Ensure PEM has real newlines and header/body/footer on separate lines.
    Converts literal '\\n' / '\\r\\n' sequences into actual newlines, then fixes spacing.
    Repairs one-line BEGIN...BODY...END pastes by re-wrapping the body.
    """
    if not isinstance(pem, str):
        raise RuntimeError("private_key_pem must be a string")

    # Convert escaped sequences (JSON style) into real newlines
    if "\\r\\n" in pem or "\\n" in pem or "\\r" in pem:
        pem = pem.replace("\\r\\n", "\n").replace("\\n", "\n").replace("\\r", "\n")

    # Normalize platform newlines, trim
    pem = pem.replace("\r\n", "\n").replace("\r", "\n").strip()

    # If it already has BEGIN/END and newlines, just ensure spacing is correct
    if "-----BEGIN" in pem and "-----END" in pem and "\n" in pem:
        pem = re.sub(r"(-----BEGIN [A-Z0-9 ]+-----)\s*", r"\1\n", pem)
        pem = re.sub(r"\s*(-----END [A-Z0-9 ]+-----)", r"\n\1", pem)
        return pem if pem.endswith("\n") else pem + "\n"

    # One-line paste fallback: rebuild body with 70-char wrapping
    one = re.sub(r"\s+", "", pem)
    m = re.search(r"-----BEGIN([A-Z0-9 ]+)-----(.+)-----END\1-----", one)
    if m:
        kind = m.group(1).strip()
        body = re.sub(r"\s+", "", m.group(2))
        wrapped = "\n".join(textwrap.wrap(body, 70))
        return f"-----BEGIN {kind}-----\n{wrapped}\n-----END {kind}-----\n"

    # Last resort: trailing newline
    return pem + ("\n" if not pem.endswith("\n") else "")

def _sha256_fingerprint(pkey: paramiko.PKey) -> str:
    digest = hashlib.sha256(pkey.asbytes()).digest()
    return "SHA256:" + base64.b64encode(digest).decode("ascii").rstrip("=")

def _load_pkey_from_secret(secret: dict) -> paramiko.PKey | None:
    """
    Load paramiko.PKey from secret fields:
      - private_key_pem (string; may contain literal \n)
      - private_key_b64 (base64 of the file bytes)
      - passphrase (optional)
    Returns PKey or None if no key present in secret.
    """
    pem = _val(secret, "private_key_pem", "privateKeyPem", "private_key")
    b64 = _val(secret, "private_key_b64", "privateKeyB64")
    passphrase = _normalize_passphrase(_val(secret, "passphrase", "key_passphrase"))
    pass_set = passphrase is not None

    def _try_paramiko_with_pem(pem_text: str):
        buf = tempfile.SpooledTemporaryFile(mode="w+")
        buf.write(pem_text)
        buf.seek(0)
        for key_cls in (paramiko.RSAKey, paramiko.ECDSAKey, paramiko.Ed25519Key):
            buf.seek(0)
            try:
                return key_cls.from_private_key(buf, password=passphrase)
            except Exception:
                continue
        return None

    def _cryptography_to_traditional_pem(raw_bytes: bytes) -> str:
        # Try with provided passphrase first (if any), then without.
        for pwd in ([passphrase.encode("utf-8")] if pass_set else [None]) + [None]:
            try:
                key_obj = serialization.load_ssh_private_key(raw_bytes, password=pwd)
            except Exception:
                try:
                    key_obj = serialization.load_pem_private_key(raw_bytes, password=pwd)
                except Exception:
                    continue
            return key_obj.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            ).decode("utf-8")
        return ""

    # PEM string route
    if pem:
        pem_fixed = _normalize_pem_text(pem)
        pk = _try_paramiko_with_pem(pem_fixed)
        if pk:
            return pk
        compat = _cryptography_to_traditional_pem(pem_fixed.encode("utf-8"))
        if compat:
            pk = _try_paramiko_with_pem(compat)
            if pk:
                return pk

    # Base64 route
    if b64:
        try:
            raw = base64.b64decode(b64)
            compat = _cryptography_to_traditional_pem(raw)
            if compat:
                pk = _try_paramiko_with_pem(compat)
                if pk:
                    return pk
        except Exception:
            pass

    return None

# Make SFTP connect fail fast; let retry_utils control attempts.
CONNECT_TIMEOUT = float(os.getenv("CONNECT_TIMEOUT", "6"))
BANNER_TIMEOUT  = float(os.getenv("BANNER_TIMEOUT",  "10"))
AUTH_TIMEOUT    = float(os.getenv("AUTH_TIMEOUT",    "10"))
SOCKET_TIMEOUT  = int(os.getenv("SOCKET_TIMEOUT",    "10"))

@default_retry()  # uses your shared retry/backoff policy
def create_sftp_client(host: str, port: int, username: str,
                       password: str | None = None,
                       pkey: paramiko.PKey | None = None,
                       hostkey_b64: str | None = None,
                       hostkey_sha256: str | None = None):
    """
    Robust SFTP connect using SSHClient with explicit timeouts and keepalive.
    Supports password OR key-based auth. Optional host-key pin via base64 or SHA256.
    """
    if not (password or pkey):
        raise RuntimeError("create_sftp_client requires either password or pkey")

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    client.connect(
        hostname=host,
        port=int(port),
        username=username,
        password=password,
        pkey=pkey,
        look_for_keys=False,
        allow_agent=False,
        timeout=CONNECT_TIMEOUT,
        banner_timeout=BANNER_TIMEOUT,
        auth_timeout=AUTH_TIMEOUT,
    )

    transport = client.get_transport()
    transport.set_keepalive(10)

    # Host-key pinning (either exact base64 or SHA256:... digest)
    server_key = base64.b64encode(transport.get_remote_server_key().asbytes()).decode()
    if hostkey_b64 and server_key != hostkey_b64:
        client.close()
        raise paramiko.SSHException("Host key pin (base64) failed")
    if hostkey_sha256:
        seen_sha = _sha256_fingerprint(transport.get_remote_server_key())
        if seen_sha != hostkey_sha256:
            client.close()
            raise paramiko.SSHException(f"Host key pin (SHA256) failed. Expected {hostkey_sha256}, got {seen_sha}")

    return client.open_sftp(), client

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
    src_client = None

    try:
        # ---- Config & Secrets ----
        src_secret_name = os.getenv('SRC_SECRET_NAME')
        if not src_secret_name:
            raise RuntimeError("SRC_SECRET_NAME env var is required")

        box_secret_name = os.getenv('BOX_SECRET_NAME')
        if not box_secret_name:
            raise RuntimeError("BOX_SECRET_NAME env var is required")

        box_folder_id = os.getenv('BOX_FOLDER_ID')
        if not box_folder_id:
            raise RuntimeError("BOX_FOLDER_ID env var is required")

        s3_bucket = os.getenv('S3_BUCKET', 'jams-ftp-process-bucket')
        s3_prefix = os.getenv('S3_PREFIX', 'ftp-listings')
        s3_kms_key_arn = os.getenv('S3_KMS_KEY_ARN')  # optional

        # Use a single anchored date folder for both S3 and Box (default: YESTERDAY)
        anchor = os.getenv("ANCHOR_DATE", "YESTERDAY").upper()
        if anchor == "YESTERDAY":
            date_folder = get_yesterday_label()
        elif anchor in ("PREV_BIZDAY", "PREVIOUS_BUSINESS_DAY", "PREV_BUSINESS_DAY"):
            date_folder = get_prev_business_day_label()
        else:
            date_folder = get_today_label()
        logger.info("[%s] Using folder '%s' anchored to ANCHOR_DATE=%s for both S3 and Box", trace_id, date_folder, anchor)

        src_secret = get_secret(src_secret_name)
        secret_host = _val(src_secret, 'Host', 'host')
        src_host = get_effective_sftp_host(secret_host)
        src_user = _val(src_secret, 'Username', 'username')
        src_pass = _val(src_secret, 'Password', 'password')
        src_port = int(_val(src_secret, 'Port', 'port', default=22))
        src_dir  = os.getenv('SRC_REMOTE_DIR', '.')

        # Optional key-based auth (preferred if present)
        pkey = _load_pkey_from_secret(src_secret)

        if not (src_host and src_user and (src_pass or pkey)):
            raise RuntimeError(
                f"Missing required SFTP fields in secret {src_secret_name} "
                f"(Host/Username and Password or Private Key)."
            )

        logger.info("[%s] SFTP host (effective)=%s port=%s (secret host=%s) auth=%s",
                    trace_id, src_host, src_port, secret_host, "pkey" if pkey else "password")

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
                msg = f"Network preflight failed to {src_host}:{src_port} – check VPC/NAT/SG/NACL"
                errors.append(msg)
                log_error(trace_id, msg)
                raise RuntimeError(msg)

            # ---- Connect to SFTP ----
            try:
                src_sftp, src_client = create_sftp_client(
                    src_host, src_port, src_user,
                    password=src_pass if not pkey else None,
                    pkey=pkey,
                    hostkey_b64=os.getenv("SSH_SERVER_KEY_B64"),
                    hostkey_sha256=os.getenv("HOST_KEY_SHA256"),
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
                # Log a quick sample of remote names
                logger.info("[%s] Sample remote files: %s%s",
                            trace_id, ", ".join(all_files[:10]),
                            " ..." if len(all_files) > 10 else "")
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

                    # S3 upload (TODAY folder + optional KMS)
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
                if src_client:
                    src_client.close()
                log_sftp_connection(trace_id, src_host, "CLOSED")
            except Exception:
                pass

            # ---- Prepare for Box ----
            box_files = match_files(os.listdir(tmp_dir), include_patterns=file_patterns)
            unmatched_box = set(os.listdir(tmp_dir)) - set(box_files)
            log_matched_files(trace_id, box_files, unmatched_box)

            if box_files:
                box_tmp_dir = os.path.join(tmp_dir, "boxonly")
                os.makedirs(box_tmp_dir, exist_ok=True)
                for fname in box_files:
                    shutil.copy2(os.path.join(tmp_dir, fname), os.path.join(box_tmp_dir, fname))

                if dry_run_enabled:
                    log_dry_run_action(trace_id, f"Would upload files {box_files} to Box folder '{date_folder}'")
                else:
                    try:
                        # Import all three here if you prefer late-imports
                        from storage_utils import (
                            upload_files_to_box_today,
                            upload_files_to_box_yesterday,
                            upload_files_to_box_prev_bizday,
                        )

                        # Anchor routing: default in your env is likely YESTERDAY or PREV_BIZDAY
                        if anchor == "YESTERDAY":
                            upload_files_to_box_yesterday(
                                box_client, box_folder_id, box_tmp_dir, {"trace_id": trace_id}
                            )
                        elif anchor in ("PREV_BIZDAY", "PREVIOUS_BUSINESS_DAY", "PREV_BUSINESS_DAY"):
                            upload_files_to_box_prev_bizday(
                                box_client, box_folder_id, box_tmp_dir, {"trace_id": trace_id}
                            )
                        else:
                            upload_files_to_box_today(
                                box_client, box_folder_id, box_tmp_dir, {"trace_id": trace_id}
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
            s3_files=list(match_files(os.listdir('/tmp')) if os.path.exists('/tmp') else []),
            box_files=box_files,
            checksum_results=[{"file": k, "status": v} for k, v in {}.items()],
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
