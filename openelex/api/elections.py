import json
import re
from .base import get

def find(state, date):
    response = get(resource_type='election', state=state, date=date)
    if response.status_code == 200:
        payload = response.json()['objects']
    else:
        msg = "Request raised error: %s (state: %s, datefilter: %s)"
        payload =  msg % (response.status_code, state, date)
    return payload
