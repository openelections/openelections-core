"""
Reference functions for Maryland political geographies as they are referenced in URLs at http://www.elections.state.md.us/
"""

def jurisdictions():
    """Maryland counties, plus Baltimore City"""
    m = self.jurisdiction_mappings(('ocd_id','fips','url_name'))
    mappings = [x for x in m if x['url_name'] is not None]
    return mappings
