import os
import re
import datetime as dt
import logging
from boxsdk.exception import BoxAPIException
from trace_utils import get_or_create_trace_id
from retry_utils import default_retry

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover (Lambda is 3.12 so this should exist)
    ZoneInfo = None

logger = logging.getLogger(__name__)


def _now_local():
    tzname = os.getenv("TIMEZONE", "America/Toronto")
    if ZoneInfo:
        return dt.datetime.now(ZoneInfo(tzname))
    return dt.datetime.now()


def get_date_subpath():
    """Current date in YYYY/MM/DD — unchanged helper."""
    now = _now_local()
    return f"{now.year}/{str(now.month).zfill(2)}/{str(now.day).zfill(2)}"


def get_prev_business_day_str():
    """
    Returns today's business day (YYYYMMDD):
      - If today is Mon–Fri: today.
      - If today is Sat/Sun: last Friday.
    (Kept for backward compatibility and as a safe fallback.)
    """
    today = _now_local().date()
    d = today
    while d.weekday() >= 5:  # 5=Sat, 6=Sun
        d -= dt.timedelta(days=1)
    return f"{d.year}{d.month:02d}{d.day:02d}"


def extract_yyyymmdd_from_name(filename: str) -> str | None:
    """
    Pull YYYYMMDD from a filename like '..._20251105182302.csv' -> '20251105'.
    We look for an 8-digit date (year >= 2000) and validate it.
    """
    m = re.search(r'(20\d{6})', filename)  # prefer 2000-2099
    if not m:
        return None
    yyyymmdd = m.group(1)
    try:
        dt.datetime.strptime(yyyymmdd, "%Y%m%d")
        return yyyymmdd
    except ValueError:
        return None


def _get_or_create_folder(client, parent_folder, folder_name):
    """Get or create a subfolder with the given name under parent_folder."""
    for item in parent_folder.get_items():
        if item.type == 'folder' and item.name == folder_name:
            return item
    return parent_folder.create_subfolder(folder_name)


@default_retry()
def upload_files_to_box_prev_bday(client, root_folder_id, local_dir, context=None):
    """
    Backward-compatible name — now uploads into folder(s) determined by
    the YYYYMMDD found in each file's name. If a file lacks a valid date,
    we fall back to today's business day.

    - Groups files by date key.
    - If a name already exists in a date folder, upload as a new version.
    """
    trace_id = get_or_create_trace_id(context)
    all_files = [f for f in os.listdir(local_dir) if os.path.isfile(os.path.join(local_dir, f))]
    if not all_files:
        logger.info(f"[{trace_id}] No files to upload to Box from {local_dir}")
        return

    # Group files by date key
    files_by_key = {}
    for fname in all_files:
        key = extract_yyyymmdd_from_name(fname) or get_prev_business_day_str()
        files_by_key.setdefault(key, []).append(fname)

    root = client.folder(root_folder_id)

    for key, files in files_by_key.items():
        logger.info(f"[{trace_id}] Uploading {len(files)} file(s) to Box folder {root_folder_id}/{key}")
        day_folder = _get_or_create_folder(client, root, key)

        # Build a quick index of existing items to support versioning flow
        existing = {item.name: item for item in day_folder.get_items(limit=1000)}

        for filename in files:
            local_path = os.path.join(local_dir, filename)
            logger.info(f"[{trace_id}] Uploading {filename} to Box {key}")
            try:
                if filename in existing:
                    # New version
                    existing[filename].update_contents(local_path)
                    logger.info(f"[{trace_id}] Uploaded {filename} as a new version in Box.")
                else:
                    with open(local_path, 'rb') as file_stream:
                        day_folder.upload_stream(file_stream, filename)
                        logger.info(f"[{trace_id}] Uploaded {filename} to Box successfully.")
            except BoxAPIException as e:
                # Defensive: handle both conflict and other errors
                if e.status == 409:
                    try:
                        # Try versioning path if conflict
                        existing = {item.name: item for item in day_folder.get_items(limit=1000)}
                        if filename in existing:
                            existing[filename].update_contents(local_path)
                            logger.info(f"[{trace_id}] (409) Uploaded {filename} as a new version in Box.")
                        else:
                            raise
                    except Exception as inner:
                        logger.error(f"[{trace_id}] Failed to resolve Box conflict for {filename}: {inner}")
                else:
                    logger.error(f"[{trace_id}] Failed to upload {filename} to Box: {e}")
