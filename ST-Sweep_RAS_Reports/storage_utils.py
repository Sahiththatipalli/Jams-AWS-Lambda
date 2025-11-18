import os
import datetime
import logging
from typing import Optional, Set, List
from boxsdk.exception import BoxAPIException
from retry_utils import default_retry

logger = logging.getLogger(__name__)

# zoneinfo is in stdlib on 3.9+. Fall back to UTC if tz DB not present.
try:
    from zoneinfo import ZoneInfo  # type: ignore
except Exception:  # pragma: no cover
    ZoneInfo = None

# ---------- Time helpers ----------

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

def get_prev_business_day(tz_name: Optional[str] = None) -> datetime.date:
    now = _now_in_tz(tz_name)
    d = now.date() - datetime.timedelta(days=1)
    holidays = _parse_business_holidays()
    while d.weekday() >= 5 or d in holidays:  # 5=Sat,6=Sun
        d -= datetime.timedelta(days=1)
    return d

def get_current_day_label(fmt: Optional[str] = None, tz_name: Optional[str] = None) -> str:
    """Default YYYYMMDD unless DATE_FOLDER_FORMAT env overrides."""
    fmt = fmt or os.getenv("DATE_FOLDER_FORMAT", "%Y%m%d")
    return _now_in_tz(tz_name).date().strftime(fmt)

def get_current_year_reports_folder_name(tz_name: Optional[str] = None, suffix: str = "Reports") -> str:
    """e.g., '2025Reports' from today's date."""
    year = _now_in_tz(tz_name).date().strftime("%Y")
    return f"{year}{suffix}"

# ---------- Pattern expansion ----------

def render_date_tokens(tmpl: str, dt: datetime.date) -> str:
    """
    Replace date tokens in `tmpl` with values from dt.
    Supported: {YYYYMMDD}, {YYYY-MM-DD}, {YYMMDD}, {YYYY}, {YY}, {MM}, {DD}
    """
    replacements = {
        "{YYYYMMDD}": dt.strftime("%Y%m%d"),
        "{YYYY-MM-DD}": dt.strftime("%Y-%m-%d"),
        "{YYMMDD}": dt.strftime("%y%m%d"),
        "{YYYY}": dt.strftime("%Y"),
        "{YY}": dt.strftime("%y"),
        "{MM}": dt.strftime("%m"),
        "{DD}": dt.strftime("%d"),
    }
    out = tmpl
    for key in ["{YYYY-MM-DD}", "{YYYYMMDD}", "{YYMMDD}", "{YYYY}", "{YY}", "{MM}", "{DD}"]:
        out = out.replace(key, replacements[key])
    return out

def expand_patterns_for_date(patterns: List[str], dt: Optional[datetime.date]) -> List[str]:
    """
    If dt is provided, render tokens in each pattern using dt; else return patterns unchanged.
    """
    if dt is None:
        return patterns
    return [render_date_tokens(p, dt) for p in patterns]

# ---------- Box helpers ----------

def _get_or_create_subfolder(parent, name: str):
    # NOTE: For very large folders, you may want to iterate get_items() with pagination.
    for item in parent.get_items():
        if item.type == "folder" and item.name == name:
            return item
    return parent.create_subfolder(name)

# add near the other date helpers
def get_current_day(tz_name: Optional[str] = None) -> datetime.date:
    """Today's date in the configured timezone (LOCAL_TZ; default America/Toronto)."""
    return _now_in_tz(tz_name).date()


@default_retry()
def ensure_box_path_and_upload(client, root_folder_id: str, path_components: List[str], local_dir: str, trace_context: str = "-"):
    """
    Ensure nested Box path exists under root_folder_id (e.g., ['2025Reports','20250917']),
    then upload all files in local_dir. On name conflict, upload as a new version.
    """
    logger.info("[%s] Ensuring Box path: %s under root %s", trace_context, "/".join(path_components), root_folder_id)
    folder = client.folder(root_folder_id)
    for comp in path_components:
        folder = _get_or_create_subfolder(folder, comp)

    files = os.listdir(local_dir)
    for filename in files:
        local_path = os.path.join(local_dir, filename)
        logger.info("[%s] Uploading %s to Box path %s", trace_context, filename, "/".join(path_components))
        with open(local_path, 'rb') as file_stream:
            try:
                folder.upload_stream(file_stream, filename)
                logger.info("[%s] Uploaded %s to Box", trace_context, filename)
            except BoxAPIException as e:
                if e.status == 409 and getattr(e, "code", None) == "item_name_in_use":
                    # Update as new version
                    items = {item.name: item for item in folder.get_items()}
                    if filename in items:
                        items[filename].update_contents(local_path)
                        logger.info("[%s] Uploaded %s as new version", trace_context, filename)
                    else:
                        logger.error("[%s] Conflict but item not found for %s", trace_context, filename)
                else:
                    logger.error("[%s] Box upload failed for %s: %s", trace_context, filename, e)
