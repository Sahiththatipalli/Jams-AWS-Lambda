import time

def time_operation(func, *args, **kwargs):
    """
    Returns (result, duration) for any callable.
    """
    t0 = time.time()
    result = func(*args, **kwargs)
    t1 = time.time()
    return result, t1 - t0
