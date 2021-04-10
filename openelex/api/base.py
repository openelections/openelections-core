"""OpenElex Api base wrapper"""
from future import standard_library
standard_library.install_aliases()
from collections import OrderedDict
from urllib.parse import urljoin
import requests

API_BASE_URL = "https://openelections.herokuapp.com/api/v1/"

def get(base_url=API_BASE_URL, resource_type='', state='', date='', params={}):
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
        get('election', 'WV', '2016-11-08')


    """ % {'base_url': API_BASE_URL}
    ordered_params = prepare_api_params(params)
    url = base_url+resource_type+"?state="+state.upper()+"&start_date="+str(date)
    response = requests.get(url, params=ordered_params, verify=False)
    return response

def prepare_api_params(params):
    """Construct ordered dict of params for API call.

    This method returns an alphabetized OrderedDict in order
    to maximize cache hits on the API.

    """
    new_params = []
    for key, val in list(params.items()):
        new_params.append((key, val))
    new_params.sort()
    ordered = OrderedDict(new_params)
    return ordered
