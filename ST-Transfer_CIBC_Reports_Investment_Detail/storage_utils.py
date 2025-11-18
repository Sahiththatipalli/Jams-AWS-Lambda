import os
import datetime
import logging
from typing import Optional, Set
from boxsdk.exception import BoxAPIException
from trace_utils import get_or_create_trace_id
from retry_utils import default_retry
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

# zoneinfo is in the stdlib on Python 3.9+, but we'll fail over to UTC if tz data isn't present.
try:
    from zoneinfo import ZoneInfo  # type: ignore
except Exception:  # pragma: no cover
    ZoneInfo = None  # Fallback later


def _now_in_tz(tz_name: Optional[str]) -> datetime.datetime:
    """
    Return 'now' in the provided IANA tz (env LOCAL_TZ default), falling back to UTC if zoneinfo unavailable.
    """
    tz_name = tz_name or os.getenv("LOCAL_TZ", "America/Toronto")
    if ZoneInfo:
        try:
            tz = ZoneInfo(tz_name)
            return datetime.datetime.now(tz)
        except Exception:
            pass
    # Fallback to UTC if tz database not available
    return datetime.datetime.utcnow()


def _parse_business_holidays() -> Set[datetime.date]:
    """
    Parse BUSINESS_HOLIDAYS env var as comma-separated YYYY-MM-DD dates to skip (e.g., statutory holidays).
    Example: BUSINESS_HOLIDAYS=2025-01-01,2025-07-01,2025-12-25
    """
    s = os.getenv("BUSINESS_HOLIDAYS", "").strip()
    days: Set[datetime.date] = set()
    if not s:
        return days
    for token in s.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            days.add(datetime.date.fromisoformat(token))
        except Exception:
            logger.warning("Invalid BUSINESS_HOLIDAYS date (YYYY-MM-DD expected): %s", token)
    return days


def get_prev_business_day(tz_name: Optional[str] = None) -> datetime.date:
    """
    Compute the previous business day:
      - Skip Saturday/Sunday
      - Skip dates listed in BUSINESS_HOLIDAYS
    """
    now = _now_in_tz(tz_name)
    d = now.date() - datetime.timedelta(days=1)
    holidays = _parse_business_holidays()
    while d.weekday() >= 5 or d in holidays:  # 5=Sat, 6=Sun
        d -= datetime.timedelta(days=1)
    return d


def get_prev_business_day_label(fmt: Optional[str] = None, tz_name: Optional[str] = None) -> str:
    """
    Return previous business day as a string. Default format is YYYY-MM-DD.
    Override with BIZDAY_FOLDER_FORMAT env var if desired.
    """
    fmt = fmt or os.getenv("BIZDAY_FOLDER_FORMAT", "%Y%m%d")
    return get_prev_business_day(tz_name).strftime(fmt)


def get_or_create_folder(client, parent_folder, folder_name):
    """
    Get or create a subfolder with the given name under the parent_folder.
    """
    for item in parent_folder.get_items():
        if item.type == 'folder' and item.name == folder_name:
            return item
    return parent_folder.create_subfolder(folder_name)


@default_retry()
def upload_files_to_box_prev_bizday(client, root_folder_id: str, local_dir: str, context=None):
    """
    Upload files into a single Box subfolder named with the previous business day
    (e.g., '2025-08-19') under the Box folder identified by root_folder_id.

    On name conflicts, uploads as a **new version** of the existing file.
    """
    trace_id = get_or_create_trace_id(context)
    folder_name = get_prev_business_day_label()
    files = os.listdir(local_dir)

    logger.info(f"[{trace_id}] Uploading {len(files)} files to Box folder {root_folder_id}/{folder_name}")
    box_root = client.folder(root_folder_id)

    # Ensure the single date-named folder exists directly under root
    day_folder = get_or_create_folder(client, box_root, folder_name)

    for filename in files:
        local_path = os.path.join(local_dir, filename)
        logger.info(f"[{trace_id}] Uploading {filename} to Box subfolder '{folder_name}'")
        with open(local_path, 'rb') as file_stream:
            try:
                day_folder.upload_stream(file_stream, filename)
                logger.info(f"[{trace_id}] Uploaded {filename} to Box successfully")
            except BoxAPIException as e:
                # If item exists, update as a new version
                if e.status == 409 and getattr(e, "code", None) == "item_name_in_use":
                    logger.info(f"[{trace_id}] {filename} exists, uploading as new version (Box versioning).")
                    items = {item.name: item for item in day_folder.get_items()}
                    if filename in items:
                        items[filename].update_contents(local_path)
                        logger.info(f"[{trace_id}] Uploaded {filename} as a new version in Box.")
                    else:
                        logger.error(f"[{trace_id}] File exists conflict, but file not found in folder: {filename}")
                else:
                    logger.error(f"[{trace_id}] Failed to upload {filename} to Box: {e}")

def get_yesterday_yyyymmdd_toronto() -> str:
    """
    Calendar 'yesterday' in America/Toronto, formatted as YYYYMMDD.
    Uses _now_in_tz() so it works even if zoneinfo is missing.
    """
    now_toronto = _now_in_tz("America/Toronto")
    yesterday = now_toronto.date() - datetime.timedelta(days=1)
    return yesterday.strftime("%Y%m%d")
