import os

import invoke
from invoke import task

from openelex import COUNTRY_DIR

FIXTURE_DIR = os.path.join(COUNTRY_DIR, 'fixtures')
COLLECTIONS = ['office', 'party',]
UPSERT_FIELDS = {
    'party': ('abbrev',),
    'office': ('state', 'name', 'district',),
}

def _get_fixture_filename(collection, fmt='csv'):
    return os.path.join(FIXTURE_DIR, "%s.%s" % (collection, fmt))

def _mongoimport_cmd(db, collection, filename=None, fmt='csv'):
    if filename is None:
        filename = _get_fixture_filename(collection)

    upsert_fields = ",".join(UPSERT_FIELDS[collection])

    cmd = ("mongoimport --db %s --collection %s --upsert --upsertFields=%s "
           "--type %s --file %s" %
           (db, collection, upsert_fields, fmt, filename))

    if fmt == 'csv':
        cmd += ' --headerline'
    return cmd
           

@task(help={
    'collection': 'Collection where metadata will be loaded. E.g. "office"',
    'filename': ("Filename of fixture file. Optional. If omitted the default "
                 "filename will be calculated based on the collection name."),
    'database': "Database where data will be loaded. Optional. Default is openelex",
})
def run(collection, filename=None, database='openelex'):
    """
    Populate metadata in MongoDB from fixture files.

    """
    if collection not in COLLECTIONS:
        raise ValueError("Unknown collection '%s'." % (collection))

    fmt = 'csv'
    cmd = _mongoimport_cmd(database, collection, filename, fmt)
    invoke.run(cmd, echo=True)
