def inject_test_error(errors_list, enabled=False):
    """
    Injects a test error message into the provided errors list if enabled.
    
    Parameters:
    - errors_list: list to append error messages to
    - enabled: bool flag to enable/disable error injection
    
    Usage:
        errors = []
        inject_test_error(errors, enabled=True)
    """
    if enabled:
        errors_list.append("Test error: Simulated file transfer failure for alert testing")
