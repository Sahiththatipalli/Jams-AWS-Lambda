import uuid

def get_or_create_trace_id(context=None):
    """
    Generates or extracts a unique trace ID per invocation.
    """
    # Use AWS Request ID as trace_id if available for better traceability
    if context and hasattr(context, 'aws_request_id'):
        return context.aws_request_id
    return str(uuid.uuid4())
