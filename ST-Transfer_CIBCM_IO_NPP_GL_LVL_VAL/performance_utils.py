# performance_utils.py

import os
import time
import logging

logger = logging.getLogger(__name__)

def get_tmp_usage(tmp_dir):
    total_size = 0
    file_count = 0
    for root, dirs, files in os.walk(tmp_dir):
        for fname in files:
            fpath = os.path.join(root, fname)
            try:
                total_size += os.path.getsize(fpath)
                file_count += 1
            except Exception:
                pass
    total_size_mb = total_size / (1024 * 1024)
    return round(total_size_mb, 2), file_count

def log_tmp_usage(tmp_dir, trace_id=None, message=""):
    size_mb, num_files = get_tmp_usage(tmp_dir)
    log_prefix = f"[{trace_id}] " if trace_id else ""
    logger.info(f"{log_prefix}Temp dir usage {message}: {size_mb} MB, {num_files} files")

def time_operation(fn, *args, **kwargs):
    start = time.time()
    result = fn(*args, **kwargs)
    end = time.time()
    duration = end - start
    return result, duration

def calc_speed(bytes_transferred, duration_sec):
    if duration_sec == 0:
        return 0.0
    mb = bytes_transferred / (1024 * 1024)
    return round(mb / duration_sec, 2)

def log_speed(action, filename, bytes_transferred, duration_sec, trace_id=None):
    speed = calc_speed(bytes_transferred, duration_sec)
    log_prefix = f"[{trace_id}] " if trace_id else ""
    logger.info(f"{log_prefix}{action} {filename}: {speed} MB/s ({bytes_transferred} bytes in {round(duration_sec,2)} sec)")
