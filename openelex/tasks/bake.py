from datetime import datetime
import re
import sys

from invoke import task

from openelex.base.bake import Baker, RawBaker
from .utils import load_module

@task
def state_file(state, fmt='csv', outputdir=None, datefilter=None,
    election_type=None, level=None, raw=False):
    """
    Writes election and candidate data, along with a manifest to structured
    files.

    Args:
        state: Required. Postal code for a state.  For example, "md".
        fmt: Format of output files.  This can be "csv" or "json".  Defaults
          to "csv".
        outputdir: Directory where output files will be written. Defaults to 
            "openelections/us/bakery"
        datefilter: Date specified in "YYYY" or "YYYY-MM-DD" used to filter
            elections before they are baked.
        election_type: Election type. For example, general, primary, etc. 
        level: Reporting level of the election results.  For example, "state",
            "county", "precinct", etc. Value must be one of the options
            specified in openelex.models.Result.REPORTING_LEVEL_CHOICES.
        raw: Bake RawResult records instead of cleaned and transformed results.

    """
    # TODO: Decide if datefilter should be required due to performance
    # considerations.
 
    # TODO: Implement filtering by office, district and party after the 
    # the data is standardized

    # TODO: Filtering by election type and level

    timestamp = datetime.now()

    filter_kwargs = {}
    if election_type:
        filter_kwargs['election_type'] = election_type

    if level:
        filter_kwargs['reporting_level'] = level

    if raw:
        baker = RawBaker(state=state, datefilter=datefilter, **filter_kwargs)
    else:
        baker = Baker(state=state, datefilter=datefilter, **filter_kwargs)

    baker.collect_items() \
         .write(fmt, outputdir=outputdir, timestamp=timestamp) \
         .write_manifest(outputdir=outputdir, timestamp=timestamp)

def get_elections(state, datefilter=None):
    """Get all election dates and types for a state"""
    state_mod_name = "openelex.us.%s" % state
    err_msg = "{} module could not be imported. Does it exist?"
    try:
        state_mod = load_module(state, ['datasource'])
    except ImportError:
        sys.exit(err_msg.format(state_mod_name))

    try:
        datasrc = state_mod.datasource.Datasource()
        elections = []
        for yr, yr_elections in datasrc.elections(datefilter).items():
            for election in yr_elections:
                elections.append((election['start_date'].replace('-', ''), election['race_type']))
        return elections
    except AttributeError:
        state_mod_name += ".datasource"
        sys.exit(err_msg.format(state_mod_name))

@task
def election_file(state, fmt='csv', outputdir=None, datefilter=None,
                  election_type=None, raw=False):
    """
    Write election and candidate data with one election per file.
    """
    timestamp = datetime.now()

    if raw:
        baker_cls = RawBaker
    else:
        baker_cls = Baker

    if datefilter is None or re.match( r'\d{4}', datefilter):
        # No date specfied, so bake all elections or date filter
        # represents a single year, so bake all elections for that year.
        elections = get_elections(state, datefilter)
    else:
        # Date filter is for a day, grab that election specifically
        if election_type is None:
            msg = "You must specify the election type when baking results for a single date."
            sys.exit(msg)
        elections = [(datefilter, election_type)]

    for election_date, election_type in elections:
        msg = "Baking results for {} election on {}\n".format(
            election_type, election_date)
        sys.stdout.write(msg)
        baker = baker_cls(state=state, datefilter=election_date,
                          election_type=election_type)

        baker.collect_items() \
             .write(fmt, outputdir=outputdir, timestamp=timestamp) \
             .write_manifest(outputdir=outputdir, timestamp=timestamp)
