# file_match_utils.py

import fnmatch
import re

def match_files(files, include_patterns=None, exclude_patterns=None, use_regex=False):
    """
    Returns files matching any include_patterns and not matching any exclude_patterns.
    Patterns can be Unix-style wildcards or regex (if use_regex=True).
    """
    if include_patterns is None:
        include_patterns = ['*']  # Default: all files
    if exclude_patterns is None:
        exclude_patterns = []

    matched = []
    for fname in files:
        # Check include
        if use_regex:
            if not any(re.search(p, fname) for p in include_patterns):
                continue
        else:
            if not any(fnmatch.fnmatch(fname, p) for p in include_patterns):
                continue
        # Check exclude
        if use_regex:
            if any(re.search(p, fname) for p in exclude_patterns):
                continue
        else:
            if any(fnmatch.fnmatch(fname, p) for p in exclude_patterns):
                continue
        matched.append(fname)
    return matched
