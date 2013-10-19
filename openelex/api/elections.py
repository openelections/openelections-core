import json
from .base import get

def find(state, datefilter=''):
    kwargs = {
        'state__postal__iexact': state.strip(),
    }
    response = get(resource_type='election', params=kwargs)
    #if datefilter:
    #    params.extend([
    #        start_date__gte'
    #    ])
    #path = "&".join(params)
    if response.status_code == 200:
        payload = json.loads(response.content)
    else:
        msg = "Request raised error: %s (state: %s, datefilter: %s)"
        payload =  msg % (response.status_code, state, datefilter)
    return payload
