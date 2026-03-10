import hashlib
import logging

def log_checksum(filepath, trace_id, algo="sha256", note=""):
    h = hashlib.new(algo)
    with open(filepath, "rb") as f:
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            h.update(chunk)
    digest = h.hexdigest()
    logging.info(f"{trace_id} Checksum ({algo}) for {filepath}: {digest} {note}")
    return digest
