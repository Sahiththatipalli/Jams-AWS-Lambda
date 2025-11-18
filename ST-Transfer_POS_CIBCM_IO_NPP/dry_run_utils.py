# dry_run_utils.py

import os
import logging

logger = logging.getLogger(__name__)

def is_dry_run_enabled():
    """
    Checks the environment variable to determine if dry run is enabled.
    Returns True if DRY_RUN is set to 'true' (case-insensitive), otherwise False.
    """
    return os.getenv('DRY_RUN', 'false').lower() == 'true'

def log_dry_run_action(action, *args):
    """
    Logs dry run actions for auditing and visibility.
    """
    logger.info(f"[DRY RUN] {action}: {args if args else ''}")
