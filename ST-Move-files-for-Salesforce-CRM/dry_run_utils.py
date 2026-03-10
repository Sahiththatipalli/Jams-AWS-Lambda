import os
import logging

logger = logging.getLogger()

def is_dry_run_enabled():
    return str(os.getenv('DRY_RUN', 'false')).lower() == 'true'

def log_dry_run_action(action_desc):
    logger.info(f"[DRY_RUN] {action_desc}")
