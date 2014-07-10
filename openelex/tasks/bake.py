from datetime import datetime
import json
import re
import sys

from invoke import task

from openelex.base.bake import Baker, RawBaker, reporting_levels_for_election
from openelex.base.publish import published_url
from .utils import load_module

BASE_HELP = {
    'state': "Two-letter state-abbreviation, e.g. NY",
    'fmt': "Format of output files.  Can be 'csv' or 'json'. Defaults is 'csv'.",
    'outputdir': ("Directory where output files will be written.  Defaults to "
        "'openelections/us/bakery'"),
    'electiontype': ("Only bake results for election of this type. "
        "Can be 'primary' or 'general'. Default is to bake results for all "
        "types of election"),
    'level': ("Only bake results aggregated at this reporting level. "
        "Values can be things like 'precinct' or 'county'.  "
        "Default is to bake results for all reporting levels."),
    'raw': "Bake raw results.  Default is to bake cleaned/standardized results",
}

STATE_FILE_HELP = BASE_HELP.copy()
STATE_FILE_HELP.update({
    'datefilter': ("Portion of a YYYYMMDD date, e.g. YYYY, YYYYMM, etc. "
        "Results will only be baked for elections with a start date matching "
        "the date string."),
})

@task(help=STATE_FILE_HELP)
def state_file(state, fmt='csv', outputdir=None, datefilter=None,
    electiontype=None, level=None, raw=False):
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
        electiontype: Election type. For example, general, primary, etc. 
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
    if electiontype:
        filter_kwargs['election_type'] = electiontype

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
    """Get all elections"""
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
                elections.append(election)
        return elections
    except AttributeError:
        state_mod_name += ".datasource"
        sys.exit(err_msg.format(state_mod_name))

def get_election_dates_types(state, datefilter=None):
    """Get all election dates and types for a state"""
    return [(elec['start_date'].replace('-', ''), elec['race_type'])
            for elec in get_elections(state, datefilter)]

ELECTION_FILE_HELP = BASE_HELP.copy()
ELECTION_FILE_HELP.update({
    'datefilter': ("Day or year, specified in YYYYMMDD format. "
        "Results will only be baked for elections with a start date matching "
        "the date string.  Default is to bake results for all elections."),
})

@task(help=ELECTION_FILE_HELP)
def election_file(state, fmt='csv', outputdir=None, datefilter=None,
                  electiontype=None, level=None, raw=False):
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
        elections = get_election_dates_types(state, datefilter)
    else:
        # Date filter is for a day, grab that election specifically
        if electiontype is None:
            msg = "You must specify the election type when baking results for a single date."
            sys.exit(msg)
        elections = [(datefilter, electiontype)]

    for election_date, election_type in elections:
        if level is not None:
            reporting_levels = [level]
        else:
            reporting_levels = reporting_levels_for_election(state, election_date,
                election_type, raw)

        for reporting_level in reporting_levels:
            msg = "Baking {} level results for {} election on {}\n".format(
                reporting_level, election_type, election_date)
            sys.stdout.write(msg)
            baker = baker_cls(state=state, datefilter=election_date,
                  election_type=election_type, reporting_level=reporting_level)

            baker.collect_items()\
                 .write(fmt, outputdir=outputdir, timestamp=timestamp) \
                 .write_manifest(outputdir=outputdir, timestamp=timestamp)

def result_urls(election, raw=False):
    urls = {}
    state = election['state']['postal']
    datefilter = election['start_date'].replace('-', '')
    if raw:
        baker_cls = RawBaker
    else:
        baker_cls = Baker

    for level in reporting_levels_for_election(state, datefilter,
            election['race_type'], raw):
        filename = baker_cls.filename("csv", state=state, datefilter=datefilter,
            election_type=election['race_type'], reporting_level=level)
        urls[level] = published_url(state, filename, raw)

    return urls

@task
def results_status_json(state):
    """
    Output a JSON file describing available results for each election.

    The JSON is intended to be consumed by the results front-end website.

    Args:
        state (string): State abbreviation.
    """
    statuses = []
    for election in get_elections(state):
        status = {
            'state': election['state']['postal'],
            'start_date': election['start_date'],
            'special': election['special'],
            'year': datetime.strptime(election['start_date'], "%Y-%m-%d").year,
            'race_type': election['race_type'],
            'results': result_urls(election),
            'results_raw': result_urls(election, raw=True),
        }
        statuses.append(status)
    print json.dumps(statuses)
