# checksum_utils.py

import hashlib
import logging

logger = logging.getLogger(__name__)

def calc_checksum(file_path, algo="sha256"):
    """
    Calculate a checksum for the file at file_path.
    Supported algos: 'sha256', 'sha1', 'md5'
    """
    h = None
    algo = algo.lower()
    if algo == "sha256":
        h = hashlib.sha256()
    elif algo == "sha1":
        h = hashlib.sha1()
    elif algo == "md5":
        h = hashlib.md5()
    else:
        raise ValueError(f"Unsupported checksum algorithm: {algo}")

    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()

def log_checksum(file_path, trace_id=None, algo="sha256", note=""):
    """
    Calculate and log the checksum for a file with optional trace ID and note.
    """
    checksum = calc_checksum(file_path, algo)
    log_prefix = f"[{trace_id}] " if trace_id else ""
    logger.info(f"{log_prefix}Checksum ({algo}) for {file_path}: {checksum} {note}")
    return checksum
