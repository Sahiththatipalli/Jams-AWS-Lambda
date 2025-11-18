import os
import datetime
import logging
from typing import Optional, Set
from boxsdk.exception import BoxAPIException
from trace_utils import get_or_create_trace_id
from retry_utils import default_retry

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


def _is_business_day(d: datetime.date, holidays: Set[datetime.date]) -> bool:
    return d.weekday() < 5 and d not in holidays  # 0-4 = Mon-Fri


def get_prev_business_day(tz_name: Optional[str] = None) -> datetime.date:
    """Previous business day (skip weekends + BUSINESS_HOLIDAYS)."""
    now = _now_in_tz(tz_name)
    d = now.date() - datetime.timedelta(days=1)
    holidays = _parse_business_holidays()
    while not _is_business_day(d, holidays):
        d -= datetime.timedelta(days=1)
    return d


def get_today_business_day(tz_name: Optional[str] = None) -> datetime.date:
    """
    Today's business day:
      - If today is a weekday and not in BUSINESS_HOLIDAYS => today
      - Else => previous business day
    """
    now = _now_in_tz(tz_name)
    d = now.date()
    holidays = _parse_business_holidays()
    if _is_business_day(d, holidays):
        return d
    # Fall back to previous business day (e.g., weekend/holiday runs)
    return get_prev_business_day(tz_name)


def get_business_day_label(which: str = "PREV",
                           fmt: Optional[str] = None,
                           tz_name: Optional[str] = None) -> str:
    """
    Return business-day label as string.
      which: "TODAY" or "PREV" (case-insensitive). Default is "PREV".
      fmt:   strftime format. If None, uses env BIZDAY_FOLDER_FORMAT (default %Y%m%d)
    """
    fmt = fmt or os.getenv("BIZDAY_FOLDER_FORMAT", "%Y%m%d")
    if str(which).upper() == "TODAY":
        day = get_today_business_day(tz_name)
    else:
        day = get_prev_business_day(tz_name)
    return day.strftime(fmt)


# Backward-compatible helpers (existing imports may use these)
def get_prev_business_day_label(fmt: Optional[str] = None, tz_name: Optional[str] = None) -> str:
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
    (Backward compatible) Upload files into a Box subfolder named with the previous business day
    under the Box folder identified by root_folder_id. On name conflicts, uploads a new version.
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
