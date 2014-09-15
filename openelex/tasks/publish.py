
from blinker import signal
from invoke import task

from openelex.base.publish import GitHubPublisher

publish_help = {
    'state': "Two-letter state-abbreviation, e.g. NY",
    'datefilter': ("Portion of a YYYYMMDD date, e.g. YYYY, YYYYMM, etc. "
        "Result files will only be published if the date portion of the filename "
        "matches the date string."),
    'raw': ("Publish raw result filess.  Default is to publish cleaned/standardized "
        "result files."),
}

def log_publish_started(sender, **kwargs):
    filename = kwargs.get('filename')
    print("Publishing {}".format(filename))

def log_publish_finished(sender, **kwargs):
    filename = kwargs.get('filename')
    print("Finished publishing {}".format(filename))

@task(help=publish_help)
def publish(state, datefilter=None, raw=False):
    """
    Publish baked result files
    
    Args:
        state: Two-letter state-abbreviation, e.g. NY
        datefilter: Portion of a YYYYMMDD date, e.g. YYYY, YYYYMM, etc.
            Result files will only be published if the date portion of the
            filename matches the date string. Default is to publish all result
            files for the specified state.
        raw: Publish raw result files.  Default is to publish
            cleaned/standardized result files.
        
    """
    pre_publish = signal('pre_publish')
    pre_publish.connect(log_publish_started)
    post_publish = signal('post_publish')
    post_publish.connect(log_publish_finished)
    publisher = GitHubPublisher()
    publisher.publish(state, datefilter=datefilter, raw=raw)
