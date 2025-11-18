import hashlib
import logging

def log_checksum(filepath, trace_id, algo="sha256", note=""):
    """
    Calculate and log checksum for the given file.
    """
    h = hashlib.new(algo)
    with open(filepath, "rb") as f:
        while chunk := f.read(8192):
            h.update(chunk)
    checksum = h.hexdigest()
    logging.info(f"[{trace_id}] Checksum ({algo}) for {filepath}: {checksum} {note}")
    return checksum
