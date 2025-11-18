import time

def time_operation(fn, *args, **kwargs):
    t0 = time.time()
    result = fn(*args, **kwargs)
    t1 = time.time()
    return result, t1-t0
