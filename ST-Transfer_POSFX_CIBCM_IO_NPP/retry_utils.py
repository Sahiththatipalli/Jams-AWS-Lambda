# retry_utils.py

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import logging
from trace_utils import get_or_create_trace_id

logger = logging.getLogger(__name__)

DEFAULT_ATTEMPTS = 3
DEFAULT_WAIT_MULTIPLIER = 2
DEFAULT_WAIT_MAX = 10

def _log_with_trace(retry_state):
    trace_id = get_or_create_trace_id()
    attempt_number = retry_state.attempt_number
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    logger.warning(f"[{trace_id}] Retry attempt {attempt_number} due to: {exc}")

def default_retry():
    return retry(
        stop=stop_after_attempt(DEFAULT_ATTEMPTS),
        wait=wait_exponential(multiplier=DEFAULT_WAIT_MULTIPLIER, max=DEFAULT_WAIT_MAX),
        retry=retry_if_exception_type(Exception),
        before_sleep=_log_with_trace,
        reraise=True
    )

def custom_retry(attempts=3, wait_multiplier=2, wait_max=10, exception_types=(Exception,)):
    return retry(
        stop=stop_after_attempt(attempts),
        wait=wait_exponential(multiplier=wait_multiplier, max=wait_max),
        retry=retry_if_exception_type(exception_types),
        before_sleep=_log_with_trace,
        reraise=True
    )
