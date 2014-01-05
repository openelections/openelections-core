import os

from invoke import task, run

from openelex.base.archive import BaseArchiver
from .utils import help_text

save_msg = "Saved to S3: %s"

@task(help=help_text({'cachefile': 'Path to file in state cache directory'}))
def save(state='', datefilter='', cachefile=''):
    """Save files from cache to s3

    Supports saving:

       1) A single file using 'cachefile' argument
       2) All files in cache using 'state' argument, or a
       subset of cached files when 'datefilter' provided.

    """
    # Save individual file and return immediately
    if cachefile:
        #Determine state from cachefle name
        path = os.path.abspath(cachefile)
        state = path.split('/')[-3]
        archiver = BaseArchiver(state)
        key = archiver.save_file(path)
        #import ipdb;ipdb.set_trace()
        print(save_msg % key.key)
    # Save all files for a given state, applying datefilter if present
    elif state:
        archiver = BaseArchiver(state)
        counter = 0
        for path in archiver.local_cache.list_dir(datefilter, full_path=True):
            key = archiver.save_file(path)
            counter += 1
            print(save_msg % key.key)
        print "Saved %s files to S3" % counter
    else:
        print("Failed to supply proper arguments. No action executed.")


@task(help=help_text({'key': 'S3 file key'}))
def delete(state='', datefilter='', key=''):
    """Delete raw state files from S3

    Supports deleting:

       1) A single file using 'key' argument
       2) All files in cache using 'state' argument, or a
       subset of cached files when 'datefilter' provided.

    """
    if key:
        state = key.lstrip('/').split('/')[2].lower()
        #import ipdb;ipdb.set_trace()
        archiver = BaseArchiver(state)
        key = archiver.delete_file(key)
        print("Deleted from S3: %s" % key.key)
    elif state:
        archiver = BaseArchiver(state)
        for key in archiver.keys(datefilter):
            key = archiver.delete_file(key)
            print("Deleted from S3: %s" % key.key)
    else:
        print("Failed to supply proper arguments. No action executed.")
