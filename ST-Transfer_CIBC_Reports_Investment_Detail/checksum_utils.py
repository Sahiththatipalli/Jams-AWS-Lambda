import hashlib

def log_checksum(filepath, trace_id, algo="sha256", note=None):
    h = hashlib.new(algo)
    with open(filepath, "rb") as f:
        while True:
            chunk = f.read(8192)
            if not chunk: break
            h.update(chunk)
    checksum = h.hexdigest()
    msg = f"{trace_id} Checksum ({algo}) for {filepath}: {checksum}"
    if note:
        msg += f" {note}"
    print(msg)
    return checksum
