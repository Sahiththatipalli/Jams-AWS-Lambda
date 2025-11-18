# trace_utils.py

import os
import uuid

def get_or_create_trace_id(context=None):
    if context and hasattr(context, 'aws_request_id'):
        trace_id = context.aws_request_id
        os.environ['TRACE_ID'] = trace_id
        return trace_id

    trace_id = os.environ.get('TRACE_ID')
    if not trace_id:
        trace_id = str(uuid.uuid4())
        os.environ['TRACE_ID'] = trace_id
    return trace_id

def set_trace_id(trace_id):
    os.environ['TRACE_ID'] = trace_id
