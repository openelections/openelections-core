from datetime import datetime

from invoke import task

from openelex.base.bake import Baker

@task
def state_file(state, format='csv', outputdir=None, datefilter=None, **kwargs):
    """
    Writes election and candidate data, along with a manifest to structured
    files.

    Arguments:

    * state: Required. Postal code for a state.  For example, "md".
    * format: Format of output files.  This can be "csv" or "json".  Defaults to
      "csv".
    * outputdir: Directory where output files will be written. Defaults to 
      "openelections/us/bakery"
    * datefilter: Date specified in "YYYY" or "YYYY-MM-DD" used to filter
      elections before they are baked.
    * type: Election type. For example, general, primary, etc. 
    * level: Reporting level of the election results.  For example, "state",
      "county", "precinct", etc. Value must be one of the options specified in
      openelex.models.Result.REPORTING_LEVEL_CHOICES.

    """
    # TODO: Decide if datefilter should be required due to performance
    # considerations.
 
    # TODO: Implement filtering by office, district and party after the 
    # the data is standardized

    # TODO: Filtering by election type and level

    timestamp = datetime.now()
    baker = Baker(state=state, datefilter=datefilter)
    baker.collect_items() \
         .write(format, outputdir=outputdir, timestamp=timestamp) \
         .write_manifest(outputdir=outputdir, timestamp=timestamp)
