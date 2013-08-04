from openelex.base.fetch import BaseScraper
import 

"""
Most complete data is in precinct file, which has sheet for all counties and then sheets for each county. Likely we'll want the county
sheets because the all counties sheet also lists candidates who did not run in every county.

URL structure for precinct files: 
    General - http://www.sos.state.oh.us/sos/upload/elections/[year]/gen/[year]precinct.xlsx
    Primary - http://www.sos.state.oh.us/sos/upload/elections/[year]/pri/[year]precinct.xlsx

Although there are variations, so probably better to use Dashboard API to get download urls. Special elections need to be scraped.

"""

class FetchResults(BaseScraper):
    
            