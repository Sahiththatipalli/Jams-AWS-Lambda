import os

def is_dry_run_enabled():
    return os.getenv("DRY_RUN", "false").lower() in ("true", "1", "yes")

# --- this is test---