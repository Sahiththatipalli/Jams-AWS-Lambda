from tenacity import retry, stop_after_attempt, wait_exponential

def default_retry():
    return retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
