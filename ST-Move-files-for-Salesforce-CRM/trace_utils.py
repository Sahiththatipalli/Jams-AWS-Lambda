import uuid

def get_or_create_trace_id(context):
    if context and getattr(context, 'aws_request_id', None):
        return context.aws_request_id
    return str(uuid.uuid4())
