# dry_run_utils.py
import os
import logging

logger = logging.getLogger(__name__)

def is_dry_run_enabled() -> bool:
    return os.getenv("DRY_RUN", "false").strip().lower() in ("1", "true", "yes")

def log_dry_run_action(message: str) -> None:
    logger.info(message)
