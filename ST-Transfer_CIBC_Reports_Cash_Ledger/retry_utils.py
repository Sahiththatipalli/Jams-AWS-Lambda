from tenacity import retry, stop_after_attempt, wait_exponential

def default_retry():
    return retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=0.5, min=1, max=5))
