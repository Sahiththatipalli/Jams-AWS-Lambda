import logging
import re

class SanitizingFormatter(logging.Formatter):
    """
    Formatter that sanitizes sensitive log content.
    """
    def format(self, record):
        msg = super().format(record)
        msg = self.sanitize(msg)
        return msg

    @staticmethod
    def sanitize(msg):
        # Remove ANSI escape sequences
        msg = re.sub(r'\x1B[@-_][0-?]*[ -/]*[@-~]', '', msg)
        # Redact tokens
        msg = re.sub(r'(\"?(Authorization|authorization)\"?\s*:\s*\")[^\"]+(\")', r'\1***REDACTED***\3', msg)
        msg = re.sub(r'(\"?(access_token|refresh_token|api_key|client_secret)\"?\s*:\s*\")[^\"]+(\")', r'\1***REDACTED***\3', msg)
        msg = re.sub(r'(Authorization|authorization):\s*[\w\-\.]+', r'\1: ***REDACTED***', msg)
        msg = re.sub(r'(password|Password|pwd)\s*=\s*[^&\s]+', r'\1: ***REDACTED***', msg)
        return msg

# Setup logger and formatter
logger = logging.getLogger()
if not any(isinstance(hdlr.formatter, SanitizingFormatter) for hdlr in logger.handlers if hdlr.formatter):
    for hdlr in logger.handlers:
        hdlr.setFormatter(SanitizingFormatter('%(levelname)s %(asctime)s %(message)s'))

def log_job_start(trace_id, job_id, patterns):
    logger.info(f"[{trace_id}] [JOB {job_id}] Starting transfer: patterns {patterns}")

def log_sftp_connection(trace_id, host, action):
    logger.info(f"[{trace_id}] SFTP session {action} to {host}")

def log_matched_files(trace_id, matched, unmatched):
    logger.info(f"[{trace_id}] Matched {len(matched)} files, Unmatched: {len(unmatched)}")
    if len(matched) < 10:
        logger.info(f"[{trace_id}] Files: {matched}")

def log_checksum_ok(trace_id, filename, checksum):
    logger.info(f"[{trace_id}] [CHECKSUM OK] {filename}: {checksum}")

def log_checksum_fail(trace_id, filename, before, after):
    logger.warning(f"[{trace_id}] [CHECKSUM FAIL] {filename}: before {before} != after {after}")

def log_file_transferred(trace_id, filename, dest, duration_s, mbps=None):
    if mbps is not None:
        logger.info(f"[{trace_id}] {filename} transferred to {dest} in {duration_s:.2f}s ({mbps:.2f} MB/s)")
    else:
        logger.info(f"[{trace_id}] {filename} transferred to {dest} in {duration_s:.2f}s")

def log_box_version(trace_id, filename, box_id, version):
    logger.info(f"[{trace_id}] {filename} uploaded to Box (ID: {box_id}, Version: {version})")

def log_archive(trace_id, filename, s3_key):
    logger.info(f"[{trace_id}] Archived {filename} to S3 at {s3_key}")

def log_tmp_usage(trace_id, num_files, free_mb):
    logger.info(f"[{trace_id}] Temp usage: {num_files} files, Free: {free_mb} MB")

def log_error(trace_id, message, exc=None):
    msg = f"[{trace_id}] ERROR: {message}"
    if exc:
        msg += f" Exception: {exc}"
    logger.error(msg)

def log_warning(trace_id, message):
    logger.warning(f"[{trace_id}] WARNING: {message}")

def log_job_end(trace_id, job_id):
    logger.info(f"[{trace_id}] [JOB {job_id}] Transfer completed.")
