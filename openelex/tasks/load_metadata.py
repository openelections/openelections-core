import os
from csv import DictReader

import click

from openelex import models
from openelex import COUNTRY_DIR

FIXTURE_DIR = os.path.join(COUNTRY_DIR, 'fixtures')
COLLECTIONS = ['office', 'party',]
UPSERT_FIELDS = {
    'party': ('abbrev',),
    'office': ('state', 'name', 'district',),
}

def _get_document_class(collection):
    return getattr(models, collection.capitalize())

def _get_fixture_filename(collection, fmt='csv'):
    return os.path.join(FIXTURE_DIR, "%s.%s" % (collection, fmt))

@click.command(name='load_metadata.run', help="Populate metadata in database "
    "from fixture files")
@click.option('--collection', help='Collection where metadata will be loaded. E.g. "office"')
@click.option('--filename', help="Filename of fixture file. Optional. If omitted "
    "the default filename will be calculated based on the collection name.")
@click.option('--database', help="Database where data will be loaded. "
    "Optional. Default is openelex")
@click.option('--clear', help="Delete all records in collection before loading")
def run(collection, filename=None, database='openelex', clear=False):
    """
    Populate metadata in MongoDB from fixture files.

    """
    if collection not in COLLECTIONS:
        raise ValueError("Unknown collection '%s'." % (collection))

    fmt = 'csv'
    if filename is None:
        filename = _get_fixture_filename(collection, fmt)
    num_created = 0
    count = 0

    doc_cls = _get_document_class(collection)

    with open(filename, 'r') as f:
        # Only delete old data if we ask for it and if we can open the new
        # file. 
        if clear: 
            print "Clearing all existing records.\n"
            doc_cls.objects.delete()

        reader = DictReader(f)
        for row in reader:
            # This might not be the most efficient way to do an upsert based
            # on natural keys, but its the clearest and probably fast enough.
            o, created = doc_cls.objects.get_or_create(**row)
            
            count += 1
            if created:
                num_created += 1

    msg = "Imported %d records.\n" % (num_created)
    if (num_created < count):
        msg = msg + "%d records already in database.\n" % (count - num_created)

    print msg
