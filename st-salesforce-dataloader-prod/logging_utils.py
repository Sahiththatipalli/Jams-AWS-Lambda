import logging

def log_job_start(trace_id, job_id, patterns):
    logging.info(f"[{trace_id}] [JOB {job_id}] Starting transfer: patterns {patterns}")

def log_job_end(trace_id, job_id):
    logging.info(f"[{trace_id}] [JOB {job_id}] Transfer completed.")

def log_sftp_connection(trace_id, host, action):
    logging.info(f"[{trace_id}] SFTP session {action} to {host}")

def log_matched_files(trace_id, matched, unmatched):
    logging.info(f"[{trace_id}] Matched {len(matched)} files, Unmatched: {len(unmatched)}")
    if len(matched) < 10:
        logging.info(f"[{trace_id}] Files: {matched}")

def log_checksum_ok(trace_id, filename, checksum):
    logging.info(f"[{trace_id}] [CHECKSUM OK] {filename}: {checksum}")

def log_checksum_fail(trace_id, filename, before, after):
    logging.warning(f"[{trace_id}] [CHECKSUM FAIL] {filename}: before {before} != after {after}")

def log_file_transferred(trace_id, filename, dest, duration_s, mbps=None):
    msg = f"[{trace_id}] {filename} transferred to {dest} in {duration_s:.2f}s"
    if mbps is not None:
        msg += f" ({mbps:.2f} MB/s)"
    logging.info(msg)

def log_archive(trace_id, filename, s3_key):
    logging.info(f"[{trace_id}] Archived {filename} to S3 at {s3_key}")

def log_tmp_usage(trace_id, num_files, free_mb):
    logging.info(f"[{trace_id}] Temp usage: {num_files} files, Free: {free_mb} MB")

def log_warning(trace_id, message):
    logging.warning(f"[{trace_id}] WARNING: {message}")

def log_error(trace_id, message, exc=None):
    msg = f"[{trace_id}] ERROR: {message}"
    if exc:
        msg += f" Exception: {exc}"
    logging.error(msg)

def log_box_version(trace_id, filename, box_id, version):
    logging.info(f"[{trace_id}] {filename} uploaded to Box (ID: {box_id}, Version: {version})")

def log_box_version(trace_id, filename, box_id, version):
    logger = logging.getLogger()
    logger.info(f"[{trace_id}] {filename} uploaded to Box (ID: {box_id}, Version: {version})")

