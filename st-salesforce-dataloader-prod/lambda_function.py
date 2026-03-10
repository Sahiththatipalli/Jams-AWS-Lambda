import os
import json
import csv
import time
import logging
import datetime
import tempfile
import shutil
import zipfile
from pathlib import Path

import boto3
import requests

import jwt
from cryptography.hazmat.primitives import serialization

from boxsdk import JWTAuth, Client
from boxsdk.exception import BoxAPIException


logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")
secrets_client = boto3.client("secretsmanager")
cloudwatch_client = boto3.client("cloudwatch")


# ----------------------------
# Optional existing helper modules
# ----------------------------
try:
    from logging_utils import log_job_start, log_job_end, log_tmp_usage, log_warning, log_error
except Exception:
    def log_job_start(trace_id, job_id, patterns):
        logger.info("[%s] [JOB %s] Starting transfer: patterns %s", trace_id, job_id, patterns)

    def log_job_end(trace_id, job_id):
        logger.info("[%s] [JOB %s] Transfer completed.", trace_id, job_id)

    def log_tmp_usage(trace_id, nfiles, free_mb):
        logger.info("[%s] Temp usage: %s files, Free: %s MB", trace_id, nfiles, free_mb)

    def log_warning(trace_id, msg):
        logger.warning("[%s] WARNING: %s", trace_id, msg)

    def log_error(trace_id, msg, exc=None):
        logger.error("[%s] ERROR: %s exc=%s", trace_id, msg, exc)


try:
    from dry_run_utils import is_dry_run_enabled, log_dry_run_action
except Exception:
    def is_dry_run_enabled():
        return os.getenv("DRY_RUN", "false").lower() == "true"

    def log_dry_run_action(msg):
        logger.info("[DRY_RUN] %s", msg)


try:
    from trace_utils import get_or_create_trace_id
except Exception:
    def get_or_create_trace_id(context=None):
        return getattr(context, "aws_request_id", "no-trace-id")


try:
    from retry_utils import default_retry
except Exception:
    def default_retry():
        def deco(fn):
            return fn
        return deco


try:
    from alert_utils import send_file_transfer_sns_alert
except Exception:
    send_file_transfer_sns_alert = None


# ----------------------------
# CloudWatch metrics
# ----------------------------
def metrics_namespace() -> str:
    return os.getenv("METRICS_NAMESPACE", "NightlyImport")


def env_name() -> str:
    return os.getenv("ENV_NAME", "prod")


def metric_dimensions(file_type: str | None = None) -> list[dict]:
    dims = [{"Name": "Environment", "Value": env_name()}]
    if file_type:
        dims.append({"Name": "FileType", "Value": file_type})
    return dims


def put_metric(metric_name: str, value: float, unit: str = "Count", file_type: str | None = None):
    try:
        cloudwatch_client.put_metric_data(
            Namespace=metrics_namespace(),
            MetricData=[
                {
                    "MetricName": metric_name,
                    "Dimensions": metric_dimensions(file_type),
                    "Timestamp": datetime.datetime.utcnow(),
                    "Value": float(value),
                    "Unit": unit,
                }
            ],
        )
    except Exception as e:
        logger.warning("CloudWatch put_metric_data failed for %s: %s", metric_name, e)


# ----------------------------
# Date helpers
# ----------------------------
def previous_business_day(ref_date=None) -> datetime.date:
    if ref_date is None:
        ref_date = datetime.date.today()
    d = ref_date - datetime.timedelta(days=1)
    while d.weekday() >= 5:
        d -= datetime.timedelta(days=1)
    return d


def date_subpath_now() -> str:
    return datetime.datetime.now().strftime("%Y/%m/%d")


def yymmdd_from_date_subpath(date_subpath: str) -> str:
    parts = date_subpath.split("/")
    if len(parts) != 3:
        return datetime.datetime.now().strftime("%y%m%d")
    y, m, d = parts
    return f"{y[2:]}{m}{d}"


def yyyy_mm_dd_from_date_subpath(date_subpath: str) -> str:
    parts = date_subpath.split("/")
    if len(parts) != 3:
        return datetime.datetime.now().strftime("%Y-%m-%d")
    y, m, d = parts
    return f"{y}-{m}-{d}"


# ----------------------------
# Headers.json parsing
# ----------------------------
def load_headers_config() -> dict:
    path = os.getenv("HEADERS_JSON_PATH", "Headers.json")
    with open(path, "r") as f:
        data = json.load(f)

    raw = data.get("headers")
    if isinstance(raw, list):
        if not raw:
            raise ValueError("Headers.json 'headers' list is empty")
        cfg = raw[0]
    elif isinstance(raw, dict):
        cfg = raw
    else:
        raise ValueError("Headers.json has unexpected structure")

    logger.info("Loaded Headers.json keys: %s", list(cfg.keys()))
    return cfg


def safe_substring(record: str, start_pos: int, end_pos: int) -> str:
    if record is None:
        return ""
    length = len(record)
    if start_pos <= 0:
        start_pos = 1
    if end_pos < start_pos:
        return ""
    if start_pos > length:
        return ""
    end_index = min(end_pos, length)
    return record[start_pos - 1: end_index]


def read_text_lines(path: str, trace_id: str) -> list[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        return [line.rstrip("\r\n") for line in lines]
    except UnicodeDecodeError as e:
        log_warning(trace_id, f"UTF-8 decode failed for {path}: {e}; retry latin-1")
        with open(path, "r", encoding="latin-1", errors="replace") as f:
            lines = f.readlines()
        return [line.replace("\u00a0", " ").rstrip("\r\n") for line in lines]


def write_csv(path: str, headers: list[str], rows: list[dict]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerows(rows)


# ----------------------------
# TXT -> CSV conversions
# ----------------------------
def convert_crmexthld(raw_lines, header_defs):
    if not raw_lines:
        return [], []

    header_names = [str(h["HeaderName"]) for h in header_defs]
    starts = [int(h["StartPosition"]) for h in header_defs]
    ends = [int(h["EndPosition"]) for h in header_defs]

    rows = []
    file_date = ""
    total = len(raw_lines)

    for idx, record in enumerate(raw_lines):
        if idx == 0:
            if len(record) >= 19:
                file_date = record[11:19]
            continue
        if idx >= total - 1:
            continue

        mv = record[177:177 + 20] if len(record) >= 197 else ""
        if mv == "00000000000000000000":
            continue

        row = {}
        for s, e, name in zip(starts, ends, header_names):
            row[name] = safe_substring(record, s, e)
        row["Date__c"] = file_date
        rows.append(row)

    return header_names + ["Date__c"], rows


def convert_crmextnav(raw_lines, header_defs):
    if not raw_lines or len(raw_lines) <= 1:
        return [], []

    data_lines = raw_lines[1:]
    header_names = [str(h["HeaderName"]) for h in header_defs]
    starts = [int(h["StartPosition"]) for h in header_defs]
    ends = [int(h["EndPosition"]) for h in header_defs]

    rows = []
    total = len(data_lines)

    for idx, record in enumerate(data_lines):
        if idx >= total - 1:
            continue
        row = {}
        for s, e, name in zip(starts, ends, header_names):
            row[name] = safe_substring(record, s, e)
        rows.append(row)

    return header_names, rows


def convert_crmexttrn(raw_lines, header_defs):
    if not raw_lines:
        return [], []

    header_names = [str(h["HeaderName"]) for h in header_defs]
    starts = [int(h["StartPosition"]) for h in header_defs]
    ends = [int(h["EndPosition"]) for h in header_defs]

    rows = []
    file_date = ""
    total = len(raw_lines)

    for idx, record in enumerate(raw_lines):
        if idx == 0:
            if len(record) >= 19:
                file_date = record[11:19]
            continue
        if idx >= total - 1:
            continue

        row = {}
        for s, e, name in zip(starts, ends, header_names):
            row[name] = safe_substring(record, s, e) if e <= len(record) else ""
        row["Header_Date__c"] = file_date
        rows.append(row)

    return header_names + ["Header_Date__c"], rows


# ----------------------------
# Secrets helper
# ----------------------------
def load_secret_json(secret_name: str) -> dict:
    resp = secrets_client.get_secret_value(SecretId=secret_name)
    return json.loads(resp.get("SecretString") or "{}")


# ----------------------------
# Salesforce JWT auth
# ----------------------------
def load_salesforce_jwt_secret() -> dict:
    secret_name = os.getenv("SALESFORCE_SECRET_NAME")
    if not secret_name:
        raise RuntimeError("SALESFORCE_SECRET_NAME is required")

    data = load_secret_json(secret_name)
    required = ["login_url", "client_id", "username", "private_key_pem"]
    missing = [k for k in required if not data.get(k)]
    if missing:
        raise RuntimeError(f"Salesforce JWT secret missing keys: {missing}")

    return data


def load_private_key(private_key_pem: str, passphrase: str | None):
    password = passphrase.encode("utf-8") if passphrase else None
    return serialization.load_pem_private_key(
        private_key_pem.encode("utf-8"),
        password=password,
    )


@default_retry()
def get_salesforce_access_token_jwt(trace_id: str):
    cfg = load_salesforce_jwt_secret()
    login_url = cfg["login_url"].rstrip("/")
    token_url = f"{login_url}/services/oauth2/token"

    logger.info("[%s] SF OAuth token URL: %s", trace_id, token_url)
    logger.info("[%s] SF OAuth username: %s", trace_id, cfg["username"])

    pk = load_private_key(cfg["private_key_pem"], cfg.get("private_key_passphrase"))

    now = int(time.time())
    claims = {
        "iss": cfg["client_id"],
        "sub": cfg["username"],
        "aud": login_url,
        "exp": now + 180,
    }

    assertion = jwt.encode(claims, pk, algorithm="RS256")
    payload = {
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion": assertion,
    }

    resp = requests.post(
        token_url,
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )

    logger.info("[%s] SF JWT OAuth response status: %s", trace_id, resp.status_code)
    if resp.status_code != 200:
        logger.error("[%s] SF JWT OAuth response body: %s", trace_id, resp.text)

    resp.raise_for_status()
    data = resp.json()
    return data["access_token"], data["instance_url"]


def sf_api_version() -> str:
    return os.getenv("SF_API_VERSION", "59.0")


def sf_object(file_type: str) -> str:
    ft = file_type.upper()
    if ft == "HLD":
        return os.getenv("SF_HLD_OBJECT", "HLD__c")
    if ft == "NAV":
        return os.getenv("SF_NAV_OBJECT", "NAV__c")
    if ft == "TRN":
        return os.getenv("SF_TRN_OBJECT", "TRN__c")
    if ft == "MISSING":
        return os.getenv("SF_MISSING_OBJECT", "Nightly_Import_Missing_File__c")
    raise ValueError(f"Unknown file type: {file_type}")


# ----------------------------
# Bulk API 2.0 helpers
# ----------------------------
def bulk_create_job(instance_url: str, token: str, object_name: str, operation: str) -> str:
    url = f"{instance_url}/services/data/v{sf_api_version()}/jobs/ingest"
    body = {"object": object_name, "operation": operation, "contentType": "CSV", "lineEnding": "CRLF"}
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json", "Accept": "application/json"}
    r = requests.post(url, headers=headers, json=body, timeout=30)
    r.raise_for_status()
    return r.json()["id"]


def bulk_upload_csv(instance_url: str, token: str, job_id: str, csv_path: str):
    url = f"{instance_url}/services/data/v{sf_api_version()}/jobs/ingest/{job_id}/batches"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "text/csv", "Accept": "application/json"}
    with open(csv_path, "rb") as f:
        data = f.read()
    r = requests.put(url, headers=headers, data=data, timeout=300)
    r.raise_for_status()


def bulk_close_job(instance_url: str, token: str, job_id: str):
    url = f"{instance_url}/services/data/v{sf_api_version()}/jobs/ingest/{job_id}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json", "Accept": "application/json"}
    r = requests.patch(url, headers=headers, json={"state": "UploadComplete"}, timeout=30)
    r.raise_for_status()


def bulk_get_job(instance_url: str, token: str, job_id: str) -> dict:
    url = f"{instance_url}/services/data/v{sf_api_version()}/jobs/ingest/{job_id}"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()


def bulk_download_result(instance_url: str, token: str, job_id: str, which: str) -> str:
    url = f"{instance_url}/services/data/v{sf_api_version()}/jobs/ingest/{job_id}/{which}"
    headers = {"Authorization": f"Bearer {token}", "Accept": "text/csv"}
    r = requests.get(url, headers=headers, timeout=180)
    if r.status_code == 204:
        return ""
    r.raise_for_status()
    return r.content.decode("utf-8", errors="replace")


def run_bulk_load_csv(trace_id: str, instance_url: str, token: str, object_name: str, csv_path: str) -> dict:
    operation = os.getenv("SF_BULK_OPERATION", "insert")
    poll = int(os.getenv("SF_BULK_POLL_SECONDS", "5"))
    max_wait = int(os.getenv("SF_BULK_MAX_WAIT_SECONDS", "900"))

    job_id = bulk_create_job(instance_url, token, object_name, operation)
    logger.info("[%s] Created Bulk job %s for %s", trace_id, job_id, object_name)

    bulk_upload_csv(instance_url, token, job_id, csv_path)
    bulk_close_job(instance_url, token, job_id)

    start = time.time()
    last = None
    while True:
        info = bulk_get_job(instance_url, token, job_id)
        last = info
        state = info.get("state")
        logger.info(
            "[%s] Bulk job %s state=%s processed=%s failed=%s",
            trace_id,
            job_id,
            state,
            info.get("numberRecordsProcessed"),
            info.get("numberRecordsFailed"),
        )
        if state in ("JobComplete", "Failed", "Aborted"):
            break
        if time.time() - start > max_wait:
            log_warning(trace_id, f"Bulk job {job_id} did not complete within {max_wait}s, last state={state}")
            break
        time.sleep(poll)

    processed = int(last.get("numberRecordsProcessed") or 0) if last else 0
    failed = int(last.get("numberRecordsFailed") or 0) if last else 0

    return {
        "job_id": job_id,
        "state": last.get("state") if last else None,
        "processed": processed,
        "failed": failed,
    }


# ----------------------------
# Apex chain trigger (Apex REST)
# ----------------------------
@default_retry()
def trigger_nightlyimport_apex_chain(trace_id: str, instance_url: str, token: str) -> dict:
    url = f"{instance_url.rstrip('/')}/services/apexrest/nightlyimport/run"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json", "Accept": "application/json"}

    logger.info("[%s] Triggering Apex chain: %s", trace_id, url)
    r = requests.post(url, headers=headers, json={}, timeout=30)

    if r.status_code == 409:
        try:
            body = r.json() if r.text else {}
        except Exception:
            body = {"raw": r.text}
        logger.warning("[%s] Apex chain already running (409): %s", trace_id, body)
        return {"status_code": 409, "body": body}

    if r.status_code >= 400:
        logger.error("[%s] Apex chain trigger failed: %s %s", trace_id, r.status_code, r.text)

    r.raise_for_status()
    try:
        body = r.json() if r.text else {}
    except Exception:
        body = {"raw": r.text}
    return {"status_code": r.status_code, "body": body}


# ----------------------------
# Missing files CSV
# ----------------------------
def build_missing_files_csv(tmp_dir: str, missing_flags: dict, import_date_str: str) -> str:
    headers = ["HLD_File_Missing__c", "NAV_File_Missing__c", "TRN_File_Missing__c", "Import_Date__c"]
    row = {
        "HLD_File_Missing__c": str(bool(missing_flags.get("HLD", False))).lower(),
        "NAV_File_Missing__c": str(bool(missing_flags.get("NAV", False))).lower(),
        "TRN_File_Missing__c": str(bool(missing_flags.get("TRN", False))).lower(),
        "Import_Date__c": import_date_str,
    }
    path = os.path.join(tmp_dir, "missing_files.csv")
    write_csv(path, headers, [row])
    return path


# ----------------------------
# ZIP helpers
# ----------------------------
def zip_dir(root_dir: str, zip_path: str):
    base = os.path.dirname(root_dir)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for r, _, files in os.walk(root_dir):
            for fn in files:
                full = os.path.join(r, fn)
                rel = os.path.relpath(full, base)
                zf.write(full, rel)


# ----------------------------
# Box helpers using same pattern as your working test lambda
# ----------------------------
def get_box_client(secret_name: str) -> Client:
    resp = secrets_client.get_secret_value(SecretId=secret_name)
    cfg = json.loads(resp.get("SecretString") or "{}")
    auth = JWTAuth.from_settings_dictionary(cfg)
    auth.authenticate_instance()
    return Client(auth)


def upload_to_box(client: Client, folder_id: str, local_path: str, file_name: str) -> str:
    folder = client.folder(folder_id)
    try:
        with open(local_path, "rb") as f:
            uploaded = folder.upload_stream(f, file_name)
        return uploaded.id
    except BoxAPIException as e:
        if e.status == 409:
            for item in folder.get_items(limit=1000):
                if item.type == "file" and item.name == file_name:
                    item.update_contents(local_path)
                    return item.id
        raise


def box_upload_zip(trace_id: str, local_zip: str, zip_name: str) -> tuple[str, str | None]:
    enabled = os.getenv("BOX_UPLOAD_ENABLED", "false").lower() == "true"
    secret_name = os.getenv("BOX_SECRET_NAME")
    folder_id = os.getenv("BOX_FOLDER_ID") or os.getenv("BOX_PARENT_FOLDER_ID")

    logger.info(
        "[%s] Box config enabled=%s secret=%s folder_id=%s",
        trace_id,
        os.getenv("BOX_UPLOAD_ENABLED"),
        secret_name,
        folder_id,
    )

    if not enabled:
        return "SKIPPED", None
    if not secret_name:
        raise RuntimeError("BOX_SECRET_NAME is required when BOX_UPLOAD_ENABLED=true")
    if not folder_id:
        raise RuntimeError("BOX_FOLDER_ID (or BOX_PARENT_FOLDER_ID) is required when BOX_UPLOAD_ENABLED=true")

    logger.info("[%s] Starting Box upload for %s", trace_id, zip_name)
    client = get_box_client(secret_name)
    file_id = upload_to_box(client, folder_id, local_zip, zip_name)
    logger.info("[%s] Uploaded zip to Box folder %s as %s (file_id=%s)", trace_id, folder_id, zip_name, file_id)
    return "OK", file_id


# ----------------------------
# S3 upload helpers
# ----------------------------
def s3_upload_and_confirm(trace_id: str, bucket: str, key: str, local_path: str):
    if is_dry_run_enabled():
        log_dry_run_action(f"Would upload to s3://{bucket}/{key}")
        return

    logger.info("[%s] Uploading to s3://%s/%s", trace_id, bucket, key)
    s3_client.upload_file(local_path, bucket, key)

    meta = s3_client.head_object(Bucket=bucket, Key=key)
    logger.info(
        "[%s] Confirmed S3 object key=%s size=%s etag=%s",
        trace_id,
        key,
        meta.get("ContentLength"),
        meta.get("ETag"),
    )


# ----------------------------
# Lambda handler
# ----------------------------
def lambda_handler(event, context):
    trace_id = get_or_create_trace_id(context)
    job_id = trace_id

    moved_files = event.get("moved_files") or []
    s3_bucket = event.get("s3_bucket") or os.getenv("S3_BUCKET")
    raw_prefix = event.get("s3_prefix") or os.getenv("RAW_S3_PREFIX", "")
    csv_prefix = os.getenv("CSV_S3_PREFIX", f"{raw_prefix}/csv")
    zip_prefix = os.getenv("ZIP_S3_PREFIX", f"{csv_prefix.rstrip('/')}/zip")
    sns_topic_arn = os.getenv("SNS_TOPIC_ARN")

    date_subpath = event.get("date_subpath") or date_subpath_now()

    # Simple Box-only test mode, avoids long Salesforce load runs
    if os.getenv("BOX_TEST_ONLY", "false").lower() == "true":
        if not os.getenv("BOX_SECRET_NAME") or not (os.getenv("BOX_FOLDER_ID") or os.getenv("BOX_PARENT_FOLDER_ID")):
            raise RuntimeError("BOX_TEST_ONLY requires BOX_SECRET_NAME and BOX_FOLDER_ID (or BOX_PARENT_FOLDER_ID)")

        tmp = Path(tempfile.mkdtemp())
        p = tmp / "hello.txt"
        p.write_text(f"hello from dataloader lambda box test\ntrace_id={trace_id}\n", encoding="utf-8")
        z = tmp / f"box-test-{trace_id}.zip"
        with zipfile.ZipFile(z, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.write(str(p), "hello.txt")

        logger.info("[%s] BOX_TEST_ONLY created zip %s bytes=%s", trace_id, str(z), z.stat().st_size)
        status, file_id = box_upload_zip(trace_id, str(z), z.name)

        return {
            "statusCode": 200,
            "body": json.dumps({"trace_id": trace_id, "box_status": status, "box_file_id": file_id}),
        }

    log_job_start(trace_id, job_id, moved_files)

    if not s3_bucket or not moved_files:
        msg = "Missing s3_bucket or moved_files (event or env)"
        log_error(trace_id, msg)
        return {"statusCode": 400, "body": json.dumps({"trace_id": trace_id, "error": msg})}

    headers_config = load_headers_config()

    transfer_status = {}
    checksum_status = {}
    errors = []
    warnings = []

    # Expected file types
    expected_types = set(
        [t.strip().upper() for t in os.getenv("EXPECTED_FILE_TYPES", "HLD,NAV,TRN").split(",") if t.strip()]
    )
    require_all_expected = os.getenv("REQUIRE_ALL_FILE_TYPES", "true").lower() == "true"

    seen_types = set()
    succeeded_types = set()

    # Salesforce token once
    sf_token, sf_instance = get_salesforce_access_token_jwt(trace_id)

    # Archive naming
    run_yymmdd = yymmdd_from_date_subpath(date_subpath)
    run_yyyy_mm_dd = yyyy_mm_dd_from_date_subpath(date_subpath)
    zip_name = f"{run_yyyy_mm_dd}-{run_yymmdd}.zip"

    enable_missing = os.getenv("ENABLE_MISSING_OBJECT", "true").lower() == "true"
    import_date_str = event.get("import_date") or previous_business_day().isoformat()

    with tempfile.TemporaryDirectory() as tmp:
        free_mb = shutil.disk_usage(tmp).free // (1024 * 1024)
        log_tmp_usage(trace_id, len(os.listdir(tmp)), free_mb)

        archive_root = os.path.join(tmp, run_yymmdd)
        hld_dir = os.path.join(archive_root, "HLD")
        nav_dir = os.path.join(archive_root, "NAV")
        trn_dir = os.path.join(archive_root, "TRN")
        rep_dir = os.path.join(archive_root, "REP")
        for d in [hld_dir, nav_dir, trn_dir, rep_dir]:
            os.makedirs(d, exist_ok=True)

        for fname in moved_files:
            base = os.path.basename(fname)
            up = base.upper()

            if "CRMEXTHLD" in up:
                file_type = "HLD"
                header_key = "CRMEXTHLD"
                out_csv = "HLD.csv"
                out_txt = "CRMEXTHLD.txt"
                out_dir = hld_dir
                trace_log = "hldInsertSoapTrace.log"
            elif "CRMEXTNAV" in up:
                file_type = "NAV"
                header_key = "CRMEXTNAV"
                out_csv = "NAV.csv"
                out_txt = "CRMEXTNAV.txt"
                out_dir = nav_dir
                trace_log = "navInsertSoapTrace.log"
            elif "CRMEXTTRN" in up:
                file_type = "TRN"
                header_key = "CRMEXTTRN"
                out_csv = "TRN.csv"
                out_txt = "CRMEXTTRN.txt"
                out_dir = trn_dir
                trace_log = "trnInsertSoapTrace.log"
            else:
                warnings.append(f"Skipping unexpected file: {base}")
                continue

            seen_types.add(file_type)

            header_defs = headers_config.get(header_key)
            if not header_defs:
                errors.append(f"No Headers.json entry for {header_key}")
                continue

            raw_key = f"{raw_prefix}/{date_subpath}/{base}"
            local_txt = os.path.join(tmp, base)
            local_csv = os.path.join(tmp, "CSV", out_csv)

            logger.info("[%s] Downloading s3://%s/%s", trace_id, s3_bucket, raw_key)
            s3_client.download_file(s3_bucket, raw_key, local_txt)

            raw_lines = read_text_lines(local_txt, trace_id)

            if file_type == "HLD":
                headers, rows = convert_crmexthld(raw_lines, header_defs)
            elif file_type == "NAV":
                headers, rows = convert_crmextnav(raw_lines, header_defs)
            else:
                headers, rows = convert_crmexttrn(raw_lines, header_defs)

            logger.info("[%s] Converting %s raw_rows=%s csv_rows=%s", trace_id, base, len(raw_lines), len(rows))
            write_csv(local_csv, headers, rows)

            csv_key = f"{csv_prefix}/{date_subpath}/{out_csv}"
            logger.info("[%s] Uploading CSV to s3://%s/%s", trace_id, s3_bucket, csv_key)
            s3_client.upload_file(local_csv, s3_bucket, csv_key)

            obj = sf_object(file_type)
            bulk = run_bulk_load_csv(trace_id, sf_instance, sf_token, obj, local_csv)

            # Metrics per file type
            put_metric("RecordsProcessed", bulk.get("processed", 0) or 0, "Count", file_type=file_type)
            put_metric("RecordsFailed", bulk.get("failed", 0) or 0, "Count", file_type=file_type)

            # Success tracking for Apex chain gating
            state = (bulk.get("state") or "").strip()
            failed = int(bulk.get("failed") or 0)
            if state == "JobComplete" and failed == 0:
                succeeded_types.add(file_type)
            else:
                errors.append(f"{file_type} load not fully successful: state={state} failed={failed}")

            shutil.copyfile(local_txt, os.path.join(out_dir, out_txt))
            shutil.copyfile(local_csv, os.path.join(out_dir, out_csv))

            success_csv = ""
            failed_csv = ""
            if bulk["state"] == "JobComplete":
                success_csv = bulk_download_result(sf_instance, sf_token, bulk["job_id"], "successfulResults")
                failed_csv = bulk_download_result(sf_instance, sf_token, bulk["job_id"], "failedResults")

            Path(os.path.join(out_dir, "success.csv")).write_text(success_csv, encoding="utf-8")
            Path(os.path.join(out_dir, "error.csv")).write_text(failed_csv, encoding="utf-8")

            Path(os.path.join(out_dir, trace_log)).write_text(
                json.dumps({"trace_id": trace_id, "file": base, "bulk": bulk}, indent=2),
                encoding="utf-8",
            )

            transfer_status[file_type] = (
                f"{obj} job={bulk['job_id']} state={bulk['state']} processed={bulk['processed']} failed={bulk['failed']}"
            )

        # Auto-detect missing files from what arrived
        missing_flags = {t: (t not in seen_types) for t in expected_types}

        if enable_missing:
            missing_csv_path = build_missing_files_csv(tmp, missing_flags, import_date_str)
            obj = sf_object("MISSING")
            bulk = run_bulk_load_csv(trace_id, sf_instance, sf_token, obj, missing_csv_path)
            transfer_status["MISSING"] = (
                f"{obj} job={bulk['job_id']} state={bulk['state']} processed={bulk['processed']} failed={bulk['failed']}"
            )

        # Trigger Apex chain only after all expected file types are present and fully successful
        trigger_chain = os.getenv("TRIGGER_NIGHTLYIMPORT_CHAIN", "false").lower() == "true"
        missing_any_expected = any(bool(missing_flags.get(t)) for t in expected_types)

        if require_all_expected:
            all_present = expected_types.issubset(seen_types)
            all_succeeded = expected_types.issubset(succeeded_types)
        else:
            all_present = True
            all_succeeded = (seen_types == succeeded_types)

        ready_to_trigger = (
            trigger_chain
            and (not is_dry_run_enabled())
            and (not errors)
            and (not missing_any_expected)
            and all_present
            and all_succeeded
        )

        if ready_to_trigger:
            try:
                apex_res = trigger_nightlyimport_apex_chain(trace_id, sf_instance, sf_token)
                transfer_status["APEX_CHAIN"] = f"status={apex_res['status_code']} body={json.dumps(apex_res['body'])}"

                put_metric("ApexChainTriggered", 1, "Count")
                put_metric("ApexChainStatusCode", float(apex_res["status_code"]), "Count")

                if apex_res["status_code"] == 409:
                    warnings.append("Apex chain already running (409), no new chain started")
            except Exception as e:
                logger.exception("[%s] Apex chain trigger failed: %s", trace_id, e)
                errors.append(f"Apex chain trigger failed: {e}")
                transfer_status["APEX_CHAIN"] = "FAILED"
                put_metric("ApexChainTriggerFailed", 1, "Count")
        else:
            transfer_status["APEX_CHAIN"] = "SKIPPED"
            put_metric("ApexChainSkipped", 1, "Count")

        local_zip = os.path.join(tmp, zip_name)
        if is_dry_run_enabled():
            log_dry_run_action(f"Would create zip {local_zip} from {archive_root}")
        else:
            zip_dir(archive_root, local_zip)

        zip_key = f"{zip_prefix}/{date_subpath}/{zip_name}"
        s3_upload_and_confirm(trace_id, s3_bucket, zip_key, local_zip)
        transfer_status["s3_zip"] = f"s3://{s3_bucket}/{zip_key}"

        try:
            box_status, box_file_id = box_upload_zip(trace_id, local_zip, zip_name)
            transfer_status["box_zip"] = box_status
            if box_file_id:
                transfer_status["box_zip_file_id"] = box_file_id
        except Exception as e:
            logger.exception("[%s] Box upload failed: %s", trace_id, e)
            errors.append(f"Box upload failed: {e}")
            transfer_status["box_zip"] = "FAILED"

        free_mb = shutil.disk_usage(tmp).free // (1024 * 1024)
        log_tmp_usage(trace_id, len(os.listdir(tmp)), free_mb)

    # SNS alert only if there are errors
    if sns_topic_arn and send_file_transfer_sns_alert and errors:
        send_file_transfer_sns_alert(
            sns_topic_arn,
            trace_id,
            transfer_status=transfer_status,
            checksum_status=checksum_status,
            errors=errors,
            warnings=warnings,
            function_name=(context.function_name if context else "st-salesforce-dataloader-prod"),
        )

    log_job_end(trace_id, job_id)

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "trace_id": trace_id,
                "transfer_status": transfer_status,
                "errors": errors,
                "warnings": warnings,
            }
        ),
    }
