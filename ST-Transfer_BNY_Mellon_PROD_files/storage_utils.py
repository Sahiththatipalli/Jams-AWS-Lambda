import os
import datetime as dt
import logging
from boxsdk.exception import BoxAPIException
from trace_utils import get_or_create_trace_id
from retry_utils import default_retry

try:
    # Python 3.9+ stdlib
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover (Lambda is 3.12 so this should exist)
    ZoneInfo = None

logger = logging.getLogger(__name__)


def _now_local():
    tzname = os.getenv("TIMEZONE", "America/Toronto")
    if ZoneInfo:
        return dt.datetime.now(ZoneInfo(tzname))
    # Fallback to naive now if zoneinfo not available
    return dt.datetime.now()


def get_date_subpath():
    """Current date in YYYY/MM/DD — used for FTP path (UNCHANGED)."""
    now = _now_local()
    return f"{now.year}/{str(now.month).zfill(2)}/{str(now.day).zfill(2)}"


def get_prev_business_day_str():
    """Return previous business day (Mon–Fri) as YYYYMMDD, using TIMEZONE (default America/Toronto)."""
    today = _now_local().date()
    d = today - dt.timedelta(days=1)
    # roll back through weekend
    while d.weekday() >= 5:  # 5=Sat, 6=Sun
        d -= dt.timedelta(days=1)
    return f"{d.year}{d.month:02d}{d.day:02d}"


def _get_or_create_folder(client, parent_folder, folder_name):
    """Get or create a subfolder with the given name under parent_folder."""
    for item in parent_folder.get_items():
        if item.type == 'folder' and item.name == folder_name:
            return item
    return parent_folder.create_subfolder(folder_name)


@default_retry()
def upload_files_to_box_prev_bday(client, root_folder_id, local_dir, context=None):
    """
    Upload files into Box under a single folder named with previous business day in YYYYMMDD.
    If a file name already exists, upload as a new version.
    """
    trace_id = get_or_create_trace_id(context)
    files = [f for f in os.listdir(local_dir) if os.path.isfile(os.path.join(local_dir, f))]
    prev_key = get_prev_business_day_str()
    logger.info(f"[{trace_id}] Uploading {len(files)} files to Box folder {root_folder_id}/{prev_key}")

    root = client.folder(root_folder_id)
    day_folder = _get_or_create_folder(client, root, prev_key)

    for filename in files:
        local_path = os.path.join(local_dir, filename)
        logger.info(f"[{trace_id}] Uploading {filename} to Box {prev_key}")
        try:
            with open(local_path, 'rb') as file_stream:
                day_folder.upload_stream(file_stream, filename)
                logger.info(f"[{trace_id}] Uploaded {filename} to Box successfully")
        except BoxAPIException as e:
            if e.status == 409 and e.code == 'item_name_in_use':
                logger.info(f"[{trace_id}] {filename} exists, uploading as new version (Box versioning).")
                items = {item.name: item for item in day_folder.get_items()}
                if filename in items:
                    items[filename].update_contents(local_path)
                    logger.info(f"[{trace_id}] Uploaded {filename} as a new version in Box.")
                else:
                    logger.error(f"[{trace_id}] Conflict reported but file not listed in folder: {filename}")
            else:
                logger.error(f"[{trace_id}] Failed to upload {filename} to Box: {e}")