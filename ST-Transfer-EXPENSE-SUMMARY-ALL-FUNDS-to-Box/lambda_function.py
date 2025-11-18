# lambda_function.py

import os
import re
import json
import time
import boto3
import paramiko
import tempfile
import shutil
import logging
from datetime import datetime
from typing import List, Dict

from logging_utils import (
    log_job_start, log_job_end, log_sftp_connection, log_matched_files,
    log_checksum_ok, log_file_transferred, log_archive,
    log_tmp_usage, log_warning, log_error
)
from checksum_utils import log_checksum
from trace_utils import get_or_create_trace_id
from file_match_utils import match_files
from retry_utils import default_retry
from storage_utils import get_prev_business_day_label   # <-- UPDATED: use prev biz day
from performance_utils import time_operation
from metrics_utils import publish_file_transfer_metric, publish_error_metric
from alert_utils import send_file_transfer_sns_alert

# ---- Base logging ----
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.getLogger("paramiko").setLevel(logging.WARNING)

# --- Silence Box SDK / HTTP chatter entirely ---
def _kill_loggers(names):
    for name in names:
        lg = logging.getLogger(name)
        try:
            lg.handlers.clear()
        except Exception:
            pass
        lg.setLevel(logging.CRITICAL)
        lg.propagate = False
        lg.disabled = True

_kill_loggers((
    "boxsdk",
    "boxsdk.oauth2",
    "boxsdk.auth.jwt_auth",
    "boxsdk.client.client",
    "boxsdk.object",
    "boxsdk.session",
    "boxsdk.network",
    "boxsdk.network.default_network",
    "boxsdk.network.logging_network",
    "urllib3",
    "urllib3.connectionpool",
    "requests",
))

# ---- Dry-run helpers with fallback ----
try:
    from dry_run_utils import is_dry_run_enabled, log_dry_run_action  # type: ignore
except Exception:
    def is_dry_run_enabled() -> bool:
        return os.getenv("DRY_RUN", "false").strip().lower() in ("1", "true", "yes")
    def log_dry_run_action(message: str) -> None:
        logger.info(message)

# ---- Box SDK ----
from boxsdk import Client, OAuth2, JWTAuth
from boxsdk.exception import BoxAPIException

# ---- Constants / clients ----
MIN_FREE_MB_THRESHOLD = 100  # warn if /tmp free space below this
s3_client_primary = boto3.client('s3')  # same-account S3


# ---------- Secrets ----------
def get_secret(secret_name: str) -> dict:
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    secret = response['SecretString']
    return json.loads(secret)


# ---------- Config helpers ----------
def get_file_patterns() -> List[str]:
    val = os.getenv('FILE_PATTERN')
    if val:
        return [x.strip() for x in val.split(',') if x.strip()]
    return ['*']

def get_primary_s3_cfg() -> Dict[str, str]:
    return {
        "bucket": os.getenv('S3_BUCKET', 'ninepoint-files-prod'),
        "prefix": os.getenv('S3_PREFIX', 'Transfer-CIBC -Reports-Daily-NAV-Report/')
    }

def get_secondary_s3_cfg() -> Dict[str, str]:
    return {
        "bucket": os.getenv('SECONDARY_S3_BUCKET', 'ninepoint-etl-dev'),
        "prefix": os.getenv('SECONDARY_S3_PREFIX', 'raw/DAILY_NAV_REPORT/'),
        "assume_role_arn": os.getenv('FUNDOPS_ROLE_ARN')
    }

def get_box_cfg() -> Dict[str, str]:
    return {
        "parent_folder_id": os.getenv('BOX_PARENT_FOLDER_ID') or os.getenv('BOX_FOLDER_ID'),
        "token_secret_name": os.getenv('BOX_TOKEN_SECRET') or os.getenv('BOX_SECRET_NAME')
    }


# ---------- Date-from-filename (now unused but kept if needed elsewhere) ----------
def extract_yyyymmdd_from_filename(name: str) -> str | None:
    """
    Return YYYYMMDD from filenames like:
      - Investment_Detail_20250107.csv
      - Daily_NAV_Report_20240103060305_1238258309_220826400.csv  (YYYYMMDDHHMMSS...)
    Strategy (in order):
      1) If it looks like Daily_NAV_Report_YYYYMMDDHHMMSS..., use that YYYYMMDD.
      2) Otherwise, try an 8-digit token right before the extension.
      3) Otherwise, first valid 8-digit date anywhere in the name.
    """
    # 1) Highly specific match for Daily_NAV_Report_YYYYMMDDHHMMSS...
    m = re.search(r'Daily_NAV_Report_(\d{8})\d{6}', name)
    if m:
        token = m.group(1)
        try:
            datetime.strptime(token, "%Y%m%d")
            return token
        except ValueError:
            pass

    # 2) 8 digits right before the extension (covers Investment_Detail_YYYYMMDD.csv)
    m = re.search(r'(\d{8})(?=(?:\.[^.]+)?$)', name)
    if m:
        token = m.group(1)
        try:
            datetime.strptime(token, "%Y%m%d")
            return token
        except ValueError:
            pass

    # 3) Any valid 8-digit date anywhere in the filename
    for m in re.finditer(r'(\d{8})', name):
        token = m.group(1)
        try:
            datetime.strptime(token, "%Y%m%d")
            return token
        except ValueError:
            continue

    return None


# ---------- SFTP ----------
@default_retry()
def create_sftp_client(host: str, port: int, username: str, password: str) -> paramiko.SFTPClient:
    # NOTE: consider adding host-key verification for security.
    try:
        transport = paramiko.Transport((host, port))
        transport.connect(username=username, password=password)
        return paramiko.SFTPClient.from_transport(transport)
    except Exception as e:
        raise RuntimeError(f"SFTP connection error: {e}")


# ---------- S3 (primary, same account) ----------
def s3_key_primary(prefix: str, date_str: str, filename: str) -> str:
    prefix = (prefix or "").lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return f"{prefix}{date_str}/{filename}"

@default_retry()
def upload_to_primary_s3(local_path: str, bucket: str, prefix: str, date_str: str,
                         trace_id: str, s3_uploaded_files: List[dict]) -> float:
    key = s3_key_primary(prefix, date_str, os.path.basename(local_path))
    _, s3_duration = time_operation(s3_client_primary.upload_file, local_path, bucket, key)
    log_file_transferred(trace_id, os.path.basename(local_path), "S3-PRIMARY", s3_duration)
    log_archive(trace_id, os.path.basename(local_path), f"{bucket}/{key}")
    s3_uploaded_files.append({"bucket": bucket, "key": key})
    return s3_duration


# ---------- S3 (secondary, cross-account) ----------
def get_secondary_s3_client(assume_role_arn: str):
    if not assume_role_arn:
        return None
    sts = boto3.client("sts")
    creds = sts.assume_role(
        RoleArn=assume_role_arn,
        RoleSessionName=f"fundops-s3-writer-{int(time.time())}"
    )["Credentials"]
    session = boto3.session.Session(
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"]
    )
    return session.client("s3")

def s3_key_secondary(prefix: str, filename: str) -> str:
    prefix = (prefix or "").lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return f"{prefix}{filename}"

@default_retry()
def upload_to_secondary_s3(local_path: str, bucket: str, prefix: str, s3_client_secondary, trace_id: str,
                           s3_uploaded_files: List[dict]) -> float:
    if s3_client_secondary is None:
        raise RuntimeError("Secondary S3 client not available (assume role missing or failed).")
    key = s3_key_secondary(prefix, os.path.basename(local_path))
    _, s3_duration = time_operation(s3_client_secondary.upload_file, local_path, bucket, key)
    log_file_transferred(trace_id, os.path.basename(local_path), "S3-SECONDARY", s3_duration)
    log_archive(trace_id, os.path.basename(local_path), f"{bucket}/{key}")
    s3_uploaded_files.append({"bucket": bucket, "key": key})
    return s3_duration


# ---------- Box helpers ----------
def get_box_client_from_secret(token_secret_name: str):
    if not token_secret_name:
        return None
    cfg = get_secret(token_secret_name)

    tok = cfg.get("access_token")
    if isinstance(tok, str) and tok.strip():
        oauth = OAuth2(client_id=None, client_secret=None, access_token=tok.strip())
        return Client(oauth)

    try:
        app = cfg["boxAppSettings"]
        app_auth = app["appAuth"]
        auth = JWTAuth(
            client_id=app["clientID"],
            client_secret=app["clientSecret"],
            enterprise_id=cfg.get("enterpriseID"),
            jwt_key_id=app_auth["publicKeyID"],
            rsa_private_key_data=app_auth["privateKey"].encode("utf-8"),
            rsa_private_key_passphrase=app_auth["passphrase"].encode("utf-8"),
        )
        auth.authenticate_instance()
        return Client(auth)
    except KeyError as e:
        raise RuntimeError(
            f"Box JWT secret is missing expected key: {e}. "
            "Expected: boxAppSettings.clientID, clientSecret, "
            "boxAppSettings.appAuth.publicKeyID/privateKey/passphrase, enterpriseID"
        )

def get_or_create_box_subfolder(client: Client, parent_folder_id: str, name: str):
    root = client.folder(parent_folder_id)
    for item in root.get_items(limit=1000):
        if item.type == 'folder' and item.name == name:
            return item
    try:
        return root.create_subfolder(name)
    except BoxAPIException as e:
        if e.status == 409:
            for item in root.get_items(limit=1000):
                if item.type == 'folder' and item.name == name:
                    return item
        raise

@default_retry()
def upload_file_to_box_folder(client: Client, folder, local_path: str, filename: str, trace_id: str):
    try:
        with open(local_path, 'rb') as stream:
            folder.upload_stream(stream, filename)
        log_file_transferred(trace_id, filename, "BOX", 0.0)
        return True
    except BoxAPIException as e:
        if e.status == 409 and getattr(e, "code", None) == "item_name_in_use":
            items = {item.name: item for item in folder.get_items(limit=1000)}
            if filename in items:
                items[filename].update_contents(local_path)
                log_file_transferred(trace_id, filename, "BOX_VERSIONED", 0.0)
                return True
        raise


# ---------- Core download from SFTP ----------
@default_retry()
def download_from_sftp(sftp_client: paramiko.SFTPClient, remote_dir: str, local_dir: str,
                       file_patterns: List[str], trace_id: str,
                       warnings: List[str], errors: List[str]) -> List[str]:
    try:
        all_files = sftp_client.listdir(remote_dir)
    except Exception as e:
        err_msg = f"Failed to list files on SFTP server {remote_dir}: {e}"
        log_error(trace_id, err_msg)
        errors.append(err_msg)
        return []

    files = match_files(all_files, include_patterns=file_patterns)
    unmatched = set(all_files) - set(files)
    log_matched_files(trace_id, files, unmatched)

    if not files:
        warning_msg = f"No files matched patterns {file_patterns} in SFTP directory {remote_dir}"
        log_warning(trace_id, warning_msg)
        warnings.append(warning_msg)

    downloaded_paths: List[str] = []
    if is_dry_run_enabled():
        for filename in files:
            msg = f"[DRY RUN] Would download {filename} from SFTP to /tmp"
            logger.info(f"[{trace_id}] {msg}")
            log_dry_run_action(msg)
            downloaded_paths.append(os.path.join(local_dir, filename))
        return downloaded_paths

    for filename in files:
        remote_path = f"{remote_dir.rstrip('/')}/{filename}"
        local_path = os.path.join(local_dir, filename)
        try:
            _, _ = time_operation(sftp_client.get, remote_path, local_path)
            downloaded_paths.append(local_path)
        except Exception as e:
            err_msg = f"Failed to download {filename} from SFTP: {e}"
            log_error(trace_id, err_msg)
            errors.append(err_msg)

    return downloaded_paths


# ---------- Orchestration ----------
def lambda_handler(event, context):
    function_name = context.function_name if context else "unknown_function"
    trace_id = get_or_create_trace_id(context)
    job_id = trace_id
    file_patterns = get_file_patterns()
    log_job_start(trace_id, job_id, file_patterns)

    # === Source (SFTP) ===
    src_secret_name = os.getenv('SRC_SECRET_NAME')
    src_secret = get_secret(src_secret_name)
    src_host = src_secret['Host']
    src_user = src_secret['Username']
    src_pass = src_secret['Password']
    src_dir = os.getenv('SRC_REMOTE_DIR', '.')

    # === Destinations ===
    p_cfg = get_primary_s3_cfg()         # same-account S3 (date folder = prev biz day)
    s_cfg = get_secondary_s3_cfg()       # cross-account S3 (no date path)
    b_cfg = get_box_cfg()                # Box (date folder = prev biz day)

    # --- Previous business day (Toronto) for all S3/Box folders ---
    prev_biz_yyyymmdd = get_prev_business_day_label("%Y%m%d")
    logger.info(f"[{trace_id}] Using previous business day folder: {prev_biz_yyyymmdd}")

    # Cross-account S3 client (if role provided)
    s3_secondary_client = None
    if s_cfg["assume_role_arn"]:
        try:
            s3_secondary_client = get_secondary_s3_client(s_cfg["assume_role_arn"])
        except Exception as e:
            log_warning(trace_id, f"Could not assume FundOps role: {e}")

    logger.info(f"[{trace_id}] Function {function_name} started. Dest S3 primary: {p_cfg['bucket']}, "
                f"secondary: {s_cfg['bucket']}; Box parent: {b_cfg.get('parent_folder_id')}")

    metrics = {}
    errors: List[str] = []
    warnings: List[str] = []
    checksum_status: Dict[str, str] = {}
    s3_uploaded_files: List[dict] = []     # list of {"bucket","key"}
    box_uploaded_files: List[dict] = []    # list of {"folder_id","name"}

    # --- Box preflight (minimal; no payload dumps) ---
    try:
        if b_cfg["parent_folder_id"] and b_cfg["token_secret_name"]:
            _client = get_box_client_from_secret(b_cfg["token_secret_name"])
            me = _client.user().get()
            logger.info(f"[{trace_id}] Box user: {me.name} ({me.id})")
            _ = _client.folder(b_cfg["parent_folder_id"]).get()  # access check
            logger.info(f"[{trace_id}] Box parent folder {b_cfg['parent_folder_id']} accessible")
        else:
            logger.info(f"[{trace_id}] Box not configured (missing parent_folder_id or token_secret_name).")
    except Exception as e:
        warn = f"Box preflight failed: {e}"
        log_warning(trace_id, warn)
        warnings.append(warn)

    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Temp usage check
            free_mb = shutil.disk_usage(tmp_dir).free // (1024 * 1024)
            log_tmp_usage(trace_id, len(os.listdir(tmp_dir)), free_mb)
            if free_mb < MIN_FREE_MB_THRESHOLD:
                warnings.append(f"Low disk space on /tmp: {free_mb} MB free")

            # --- SFTP connect ---
            try:
                sftp = create_sftp_client(src_host, 22, src_user, src_pass)
                log_sftp_connection(trace_id, src_host, "OPENED")
            except Exception as e:
                err_msg = f"Failed to connect to SFTP server {src_host}: {e}"
                log_error(trace_id, err_msg)
                errors.append(err_msg)
                return {
                    'statusCode': 500,
                    'body': json.dumps({'error': err_msg, 'trace_id': trace_id})
                }

            # --- Download matched files to /tmp ---
            local_paths = download_from_sftp(
                sftp, src_dir, tmp_dir, file_patterns, trace_id, warnings, errors
            )
            try:
                sftp.close()
            finally:
                log_sftp_connection(trace_id, src_host, "CLOSED")

            # Prepare Box client & maintain a cache of date-named subfolders
            box_client = None
            box_folder_cache: Dict[str, object] = {}

            if b_cfg["parent_folder_id"] and b_cfg["token_secret_name"]:
                try:
                    box_client = get_box_client_from_secret(b_cfg["token_secret_name"])
                    # Parent folder accessibility already checked above
                except Exception as e:
                    warnings.append(f"Box not available: {e}")

            # --- For each local file: checksum, upload using PREVIOUS BUSINESS DAY folder ---
            total_bytes_primary = 0
            total_bytes_secondary = 0
            total_bytes_box = 0
            t0_primary = time.time()
            t0_secondary = time.time()
            t0_box = time.time()

            for local_path in local_paths:
                filename = os.path.basename(local_path)

                # NOTE: Date for all files is PREVIOUS BUSINESS DAY (Toronto), not from filename
                date_token = prev_biz_yyyymmdd

                # Compute checksum for reporting (single pass per file)
                try:
                    sha256 = log_checksum(local_path, trace_id, algo="sha256", note="pre-upload")
                    log_checksum_ok(trace_id, filename, sha256)
                    checksum_status[filename] = f"OK (sha256: {sha256})"
                except Exception as e:
                    log_warning(trace_id, f"Checksum failed for {filename}: {e}")
                    checksum_status[filename] = f"ERROR ({e})"

                # Primary S3 (with previous business day as folder)
                try:
                    if is_dry_run_enabled():
                        msg = (f"[DRY RUN] Would upload to S3 primary "
                               f"s3://{p_cfg['bucket']}/{p_cfg['prefix']}{date_token}/{filename}")
                        logger.info(f"[{trace_id}] {msg}")
                        log_dry_run_action(msg)
                    else:
                        _ = upload_to_primary_s3(local_path, p_cfg["bucket"], p_cfg["prefix"],
                                                 date_token, trace_id, s3_uploaded_files)
                        total_bytes_primary += os.path.getsize(local_path)
                except Exception as e:
                    err_msg = f"Primary S3 upload failed for {filename}: {e}"
                    log_error(trace_id, err_msg)
                    errors.append(err_msg)

                # Secondary S3 (no date folder — unchanged)
                try:
                    if is_dry_run_enabled():
                        msg = f"[DRY RUN] Would upload to S3 secondary s3://{s_cfg['bucket']}/{s_cfg['prefix']}{filename}"
                        logger.info(f"[{trace_id}] {msg}")
                        log_dry_run_action(msg)
                    else:
                        if s3_secondary_client is None:
                            log_warning(trace_id, "Secondary S3 client not initialized; skipping FundOps upload.")
                        else:
                            _ = upload_to_secondary_s3(local_path, s_cfg["bucket"], s_cfg["prefix"],
                                                       s3_secondary_client, trace_id, s3_uploaded_files)
                            total_bytes_secondary += os.path.getsize(local_path)
                except Exception as e:
                    err_msg = f"Secondary S3 upload failed for {filename}: {e}"
                    log_error(trace_id, err_msg)
                    errors.append(err_msg)

                # Box (previous business day subfolder)
                try:
                    if box_client and b_cfg["parent_folder_id"]:
                        if date_token not in box_folder_cache:
                            box_folder_cache[date_token] = get_or_create_box_subfolder(
                                box_client, b_cfg["parent_folder_id"], date_token
                            )
                        folder = box_folder_cache[date_token]
                        if is_dry_run_enabled():
                            msg = f"[DRY RUN] Would upload to Box folder {folder.object_id}: {filename}"
                            logger.info(f"[{trace_id}] {msg}")
                            log_dry_run_action(msg)
                        else:
                            upload_file_to_box_folder(box_client, folder, local_path, filename, trace_id)
                            box_uploaded_files.append({"folder_id": folder.object_id, "name": filename})
                            total_bytes_box += os.path.getsize(local_path)
                    else:
                        # Box not configured
                        pass
                except Exception as e:
                    err_msg = f"Box upload failed for {filename}: {e}"
                    log_error(trace_id, err_msg)
                    warnings.append(err_msg)

            # --- Metrics per destination ---
            dur_primary = max(0.0001, time.time() - t0_primary)
            mb_primary = total_bytes_primary / (1024 * 1024.0)
            metrics["S3_PRIMARY total mb"] = f"{mb_primary:.2f}"
            metrics["S3_PRIMARY mb/s"] = f"{(mb_primary/dur_primary):.2f}"

            dur_secondary = max(0.0001, time.time() - t0_secondary)
            mb_secondary = total_bytes_secondary / (1024 * 1024.0)
            metrics["S3_SECONDARY total mb"] = f"{mb_secondary:.2f}"
            metrics["S3_SECONDARY mb/s"] = f"{(mb_secondary/dur_secondary):.2f}"

            dur_box = max(0.0001, time.time() - t0_box)
            mb_box = total_bytes_box / (1024 * 1024.0)
            metrics["BOX total mb"] = f"{mb_box:.2f}"
            metrics["BOX mb/s"] = f"{(mb_box/dur_box):.2f}"

            # CloudWatch custom metrics (best-effort)
            try:
                publish_file_transfer_metric(
                    namespace='LambdaFileTransfer',
                    direction='SFTP_TO_S3_PRIMARY',
                    file_count=len(local_paths),
                    total_bytes=total_bytes_primary,
                    duration_sec=round(dur_primary, 2),
                    trace_id=trace_id
                )
                publish_file_transfer_metric(
                    namespace='LambdaFileTransfer',
                    direction='SFTP_TO_S3_SECONDARY',
                    file_count=len(local_paths),
                    total_bytes=total_bytes_secondary,
                    duration_sec=round(dur_secondary, 2),
                    trace_id=trace_id
                )
                publish_file_transfer_metric(
                    namespace='LambdaFileTransfer',
                    direction='SFTP_TO_BOX',
                    file_count=len(local_paths),
                    total_bytes=total_bytes_box,
                    duration_sec=round(dur_box, 2),
                    trace_id=trace_id
                )
            except Exception as e:
                log_warning(trace_id, f"CloudWatch metric publish warning: {e}")

        # === Build destination lists for the alert ===
        s3_list  = [f"s3://{x['bucket']}/{x['key']}" for x in s3_uploaded_files]
        box_list = [f"box://{x['folder_id']}/{x['name']}" for x in box_uploaded_files]

        # === Alerts ===
        if errors or warnings:
            try:
                send_file_transfer_sns_alert(
                    trace_id=trace_id,
                    s3_files=s3_list,
                    box_files=box_list,
                    checksum_results=[{'file': k, 'status': v} for k, v in checksum_status.items()],
                    errors=errors,
                    warnings=warnings,
                    function_name=function_name
                )
            except TypeError:
                send_file_transfer_sns_alert(
                    trace_id=trace_id,
                    s3_files=(s3_list + box_list),
                    checksum_results=[{'file': k, 'status': v} for k, v in checksum_status.items()],
                    errors=errors,
                    warnings=warnings,
                    function_name=function_name
                )
        else:
            logger.info(f"[{trace_id}] No errors or warnings detected. No alert sent.")
    except Exception as e:
        # Unexpected exception, log and alert
        err_msg = f"Unhandled exception in Lambda: {e}"
        log_error(trace_id, err_msg, exc=e)

        try:
            s3_list  = [f"s3://{x['bucket']}/{x['key']}" for x in s3_uploaded_files]
            box_list = [f"box://{x['folder_id']}/{x['name']}" for x in box_uploaded_files]
        except Exception:
            s3_list, box_list = [], []

        try:
            send_file_transfer_sns_alert(
                trace_id=trace_id,
                s3_files=s3_list,
                box_files=box_list,
                checksum_results=[{'file': k, 'status': v} for k, v in checksum_status.items()],
                errors=[err_msg],
                warnings=warnings,
                function_name=function_name
            )
        except TypeError:
            send_file_transfer_sns_alert(
                trace_id=trace_id,
                s3_files=(s3_list + box_list),
                checksum_results=[{'file': k, 'status': v} for k, v in checksum_status.items()],
                errors=[err_msg],
                warnings=warnings,
                function_name=function_name
            )
        raise

    log_job_end(trace_id, job_id)
    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Files transferred to S3 (primary & secondary) and Box using previous business day folder.', 'trace_id': trace_id})
    }
