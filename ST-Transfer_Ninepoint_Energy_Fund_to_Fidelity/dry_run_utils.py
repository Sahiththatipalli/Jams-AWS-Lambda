import os
import logging

_TRUTHY = {"1", "true", "t", "yes", "y", "on"}
_FALSY  = {"0", "false", "f", "no", "n", "off"}

def _get_env_flag(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    s = str(val).strip().lower()
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return default

def is_dry_run_enabled(event_override: dict | None = None) -> bool:
    """
    Global dry-run gate. Compatible with previous usage (no args).
    - Env: DRY_RUN = true/false (also accepts 1/0, yes/no, on/off)
    - Optional per-invocation override: event_override['dry_run']
    """
    if isinstance(event_override, dict) and 'dry_run' in event_override:
        v = event_override['dry_run']
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            return v.strip().lower() in _TRUTHY
    return _get_env_flag("DRY_RUN", False)

def get_dry_run_components() -> set:
    """
    Scope of dry-run: {'all'} or a subset of {'source','s3','dest'}.
    - Env: DRY_RUN_COMPONENTS = all | source,s3,dest
    - Defaults to 'all' when DRY_RUN is enabled; empty set when disabled.
    """
    if not is_dry_run_enabled():
        return set()
    raw = os.getenv("DRY_RUN_COMPONENTS", "all").strip().lower()
    parts = {p.strip() for p in raw.split(",") if p.strip()}
    if not parts:
        parts = {"all"}
    valid = {"all", "source", "s3", "dest"}
    chosen = parts & valid
    return chosen or {"all"}

def should_dry_run(component: str) -> bool:
    """
    Check if a specific component ('source','s3','dest') should be dry-run.
    Useful if you decide to gate parts differently in the future.
    """
    comps = get_dry_run_components()
    return "all" in comps or component.lower() in comps

def log_dry_run_action(action: str, trace_id: str | None = None) -> None:
    """
    Structured DRY-RUN logger. Keeps your existing call sites working:
      log_dry_run_action("Would do X")
    """
    prefix = "[DRY RUN]"
    msg = f"{prefix} {action}"
    if trace_id:
        msg = f"[{trace_id}] {msg}"
    try:
        logging.getLogger().info(msg)
    except Exception:
        # Fallback if logging isn't configured yet
        print(msg)

def log_dry_run_banner(trace_id=None):
    from .dry_run_utils import is_dry_run_enabled, get_dry_run_components  # if same file omit the dot
    dry = is_dry_run_enabled()
    comps = ",".join(sorted(get_dry_run_components())) if dry else "(disabled)"
    msg = f"DRY_RUN={dry} components={comps}"
    logging.getLogger().info(f"[{trace_id}] {msg}" if trace_id else msg)