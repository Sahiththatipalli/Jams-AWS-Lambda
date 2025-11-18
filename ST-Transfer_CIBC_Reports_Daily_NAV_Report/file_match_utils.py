import fnmatch

def match_files(file_list, include_patterns=None, exclude_patterns=None):
    """
    Filter files from file_list matching include_patterns and not matching exclude_patterns.
    Patterns should be standard glob patterns (e.g., '*.csv', '*.txt').
    """
    if not include_patterns:
        include_patterns = ['*']

    matched = set()
    for pattern in include_patterns:
        matched.update(fnmatch.filter(file_list, pattern))

    if exclude_patterns:
        for pattern in exclude_patterns:
            matched.difference_update(fnmatch.filter(matched, pattern))

    return sorted(list(matched))
