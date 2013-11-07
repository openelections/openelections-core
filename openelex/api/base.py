"""OpenElex Api base wrapper"""
from collections import OrderedDict
from urlparse import urljoin
import requests

API_BASE_URL = "http://openelections.net/api/v1/"
BASE_PARAMS = ['format=json', 'limit=0']

def get(base_url=API_BASE_URL, resource_type='', params={}):
    """
    Constructs API call from base url, resource type and GET
    params. Resource type should be valid endpoint for OpenElex API, 
    and params should be valid Tastypie filters for a given endpoint.

    Details on both can be explored at:

        %(base_url)s?format=json

    base_url - defaults to %(base_url)s
    resource_type - [election|state|organization], etc.
    params - dictionary of valid Tastypie filters

    USAGE:
        # Default returns list endpoints
        get()

        # Get
        get('election', {'start_date=':'2012-11-02'})


    """ % {'base_url': API_BASE_URL}
    ordered_params = prepare_api_params(params)
    url = urljoin(base_url, resource_type)
    if not url.endswith('/'):
        url += '/'
    response = requests.get(url, params=ordered_params)
    return response

def prepare_api_params(params):
    """Construct ordered dict of params for API call.

    This method returns an alphabetized OrderedDict in order
    to maximize cache hits on the API.

    """
    try:
        fmt = params.pop('format')
    except KeyError:
        fmt = 'json'

    try:
        limit = params.pop('limit')
    except KeyError:
        limit ='0'

    new_params = []
    for key, val in params.items():
        new_params.append((key, val))
    new_params.sort()
    new_params.extend([('format', fmt), ('limit', limit)])
    ordered = OrderedDict(new_params)
    return ordered
