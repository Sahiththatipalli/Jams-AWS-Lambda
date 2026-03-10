import datetime


def get_date_subpath(now: datetime.datetime | None = None) -> str:
    """Return YYYY/MM/DD based on local time."""
    if now is None:
        now = datetime.datetime.now()
    return f"{now.year}/{str(now.month).zfill(2)}/{str(now.day).zfill(2)}"