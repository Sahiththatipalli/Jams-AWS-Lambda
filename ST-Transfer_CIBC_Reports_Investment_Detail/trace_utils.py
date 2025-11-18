import uuid

def get_or_create_trace_id(context):
    # If you want to use AWS context.request_id, do so here:
    return str(uuid.uuid4())
