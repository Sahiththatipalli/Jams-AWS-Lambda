from datetime import datetime

def get_s3_date_folder():
    """
    Returns YYYYMMDD string for S3 date-based foldering.
    """
    return datetime.now().strftime('%Y%m%d')
