import os
import datetime
import logging
from typing import Optional, Set, List
from boxsdk.exception import BoxAPIException
from trace_utils import get_or_create_trace_id
from retry_utils import default_retry

logger = logging.getLogger(__name__)

try:
    from zoneinfo import ZoneInfo  # type: ignore
except Exception:  # pragma: no cover
    ZoneInfo = None

def _now_in_tz(tz_name: Optional[str]) -> datetime.datetime:
    tz_name = tz_name or os.getenv("LOCAL_TZ", "America/Toronto")
    if ZoneInfo:
        try:
            tz = ZoneInfo(tz_name)
            return datetime.datetime.now(tz)
        except Exception:
            pass
    return datetime.datetime.utcnow()

def _parse_business_holidays() -> Set[datetime.date]:
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

# ----- Previous business day (kept for compatibility) -----
def get_prev_business_day(tz_name: Optional[str] = None) -> datetime.date:
    now = _now_in_tz(tz_name)
    d = now.date() - datetime.timedelta(days=1)
    holidays = _parse_business_holidays()
    while d.weekday() >= 5 or d in holidays:
        d -= datetime.timedelta(days=1)
    return d

def get_prev_business_day_label(fmt: Optional[str] = None, tz_name: Optional[str] = None) -> str:
    fmt = fmt or os.getenv("BIZDAY_FOLDER_FORMAT", "%Y%m%d")
    return get_prev_business_day(tz_name).strftime(fmt)

# ----- Current day (used now) -----
def get_current_day(tz_name: Optional[str] = None) -> datetime.date:
    return _now_in_tz(tz_name).date()

def get_current_day_label(fmt: Optional[str] = None, tz_name: Optional[str] = None) -> str:
    fmt = fmt or os.getenv("DATE_FOLDER_FORMAT", "%Y%m%d")
    return get_current_day(tz_name).strftime(fmt)

# ----- Token rendering for FILE_PATTERN -----
def render_date_tokens(tmpl: str, dt: datetime.date) -> str:
    """
    Replace date tokens in `tmpl` with values from dt.
    Supported: {YYYYMMDD}, {YYMMDD}, {YYYY}, {YY}, {MM}, {DD}
    """
    replacements = {
        "{YYYYMMDD}": dt.strftime("%Y%m%d"),
        "{YYMMDD}": dt.strftime("%y%m%d"),
        "{YYYY}": dt.strftime("%Y"),
        "{YY}": dt.strftime("%y"),
        "{MM}": dt.strftime("%m"),
        "{DD}": dt.strftime("%d"),
    }
    out = tmpl
    # Replace longer tokens first to avoid partial overlaps
    for key in ["{YYYYMMDD}", "{YYMMDD}", "{YYYY}", "{YY}", "{MM}", "{DD}"]:
        out = out.replace(key, replacements[key])
    return out

def expand_patterns_for_today(patterns: List[str], tz_name: Optional[str] = None) -> List[str]:
    """
    Take a list like ["NINEPOINT_PCF_{YYYYMMDD}.csv"] and expand to today's actual filenames.
    If a pattern has no tokens, it is returned unchanged.
    """
    today = get_current_day(tz_name)
    return [render_date_tokens(p, today) for p in patterns]

# ----- Box helpers -----
def get_or_create_folder(client, parent_folder, folder_name):
    for item in parent_folder.get_items():
        if item.type == 'folder' and item.name == folder_name:
            return item
    return parent_folder.create_subfolder(folder_name)

@default_retry()
def upload_files_to_box_prev_bizday(client, root_folder_id: str, local_dir: str, context=None):
    trace_id = get_or_create_trace_id(context)
    folder_name = get_prev_business_day_label()
    return upload_files_to_box_folder(client, root_folder_id, local_dir, folder_name, context=context)

@default_retry()
def upload_files_to_box_folder(client, root_folder_id: str, local_dir: str, folder_name: str, context=None):
    trace_id = get_or_create_trace_id(context)
    files = os.listdir(local_dir)
    logger.info(f"[{trace_id}] Uploading {len(files)} files to Box folder {root_folder_id}/{folder_name}")
    box_root = client.folder(root_folder_id)
    day_folder = get_or_create_folder(client, box_root, folder_name)
    for filename in files:
        local_path = os.path.join(local_dir, filename)
        logger.info(f"[{trace_id}] Uploading {filename} to Box subfolder '{folder_name}'")
        with open(local_path, 'rb') as file_stream:
            try:
                day_folder.upload_stream(file_stream, filename)
                logger.info(f"[{trace_id}] Uploaded {filename} to Box successfully")
            except BoxAPIException as e:
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
