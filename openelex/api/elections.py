import json
import re
from .base import get

def find(state, year):
    response = get(resource_type='election', state=state, year=year)
    if response.status_code == 200:
        payload = response.json()
    else:
        msg = "Request raised error: %s (state: %s, datefilter: %s)"
        payload =  msg % (response.status_code, state, year)
    return payload
