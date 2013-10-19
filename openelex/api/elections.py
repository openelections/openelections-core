from .base import get

def find(state, datefilter=''):
    kwargs = {
        'state__postal__iexact=': state.strip(),
    }
    import pdb;pdb.set_trace()
    response = get('election', params=kwargs)
    #if datefilter:
    #    params.extend([
    #        start_date__gte='
    #    ])
    #path = "&".join(params)
    if response.status == 200:
        payload = json.load(response.content)
    else:
        msg = "Request raised error: %s (state: %s, datefilter: %s)"
        payload =  msg % (respose.status, state, datefiler)
    return payload
