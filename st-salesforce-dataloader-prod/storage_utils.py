# storage_utils.py

import os
import datetime
import logging
from boxsdk.exception import BoxAPIException
from trace_utils import get_or_create_trace_id
from retry_utils import default_retry

logger = logging.getLogger(__name__)

def get_date_subpath():
    now = datetime.datetime.now()
    return f"{now.year}/{str(now.month).zfill(2)}/{str(now.day).zfill(2)}"

def get_or_create_folder(client, parent_folder, folder_name):
    """
    Get or create a subfolder with the given name under the parent_folder.
    """
    for item in parent_folder.get_items():
        if item.type == 'folder' and item.name == folder_name:
            return item
    return parent_folder.create_subfolder(folder_name)

@default_retry()
def upload_files_to_box_by_date(client, root_folder_id, local_dir, context=None):
    """
    Uploads files into Box organized by Year/Month/Day, using versioning for conflicts.
    Adds trace_id to logs.
    """
    trace_id = get_or_create_trace_id(context)
    files = os.listdir(local_dir)
    logger.info(f"[{trace_id}] Uploading {len(files)} files to Box folder {root_folder_id}")
    box_root = client.folder(root_folder_id)
    parts = get_date_subpath().split('/')
    # Traverse/create Year/Month/Day folders
    folder = box_root
    for part in parts:
        folder = get_or_create_folder(client, folder, part)
    day_folder = folder

    for filename in files:
        local_path = os.path.join(local_dir, filename)
        logger.info(f"[{trace_id}] Uploading {filename} to Box {get_date_subpath()}")
        with open(local_path, 'rb') as file_stream:
            try:
                day_folder.upload_stream(file_stream, filename)
                logger.info(f"[{trace_id}] Uploaded {filename} to Box successfully")
            except BoxAPIException as e:
                if e.status == 409 and e.code == 'item_name_in_use':
                    logger.info(f"[{trace_id}] {filename} exists, uploading as new version (Box versioning).")
                    items = {item.name: item for item in day_folder.get_items()}
                    if filename in items:
                        items[filename].update_contents(local_path)
                        logger.info(f"[{trace_id}] Uploaded {filename} as a new version in Box.")
                    else:
                        logger.error(f"[{trace_id}] File exists conflict, but file not found in folder: {filename}")
                else:
                    logger.error(f"[{trace_id}] Failed to upload {filename} to Box: {e}")
