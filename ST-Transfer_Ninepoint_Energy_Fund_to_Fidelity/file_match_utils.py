import fnmatch

def match_files(files, include_patterns=None):
    """
    Returns files matching any of the glob-style include patterns.
    Keeps original order, removes duplicates.
    """
    if not include_patterns:
        return files
    matched = []
    for pattern in include_patterns:
        matched.extend(fnmatch.filter(files, pattern))
    seen = set()
    ordered = []
    for f in matched:
        if f not in seen:
            seen.add(f)
            ordered.append(f)
    return ordered