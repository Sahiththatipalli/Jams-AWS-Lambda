import fnmatch

def match_files(files, include_patterns=None):
    if not include_patterns:
        return files
    matched = []
    for pattern in include_patterns:
        matched.extend(fnmatch.filter(files, pattern))
    return sorted(set(matched))
