import click
from mongoengine import ConnectionError

from openelex.db import init_db
from fetch import fetch
from shell import shell
from publish import publish

import archive, cache, datasource, load, load_metadata, transform, validate, bake

@click.group()
def cli():
    pass

cli.add_command(archive.save)
cli.add_command(archive.delete)
cli.add_command(bake.state_file)
cli.add_command(bake.election_file)
cli.add_command(bake.results_status_json)
cli.add_command(cache.files)
cli.add_command(cache.clear)
cli.add_command(datasource.target_urls)
cli.add_command(datasource.mappings)
cli.add_command(datasource.elections)
cli.add_command(datasource.filename_url_pairs)
cli.add_command(fetch)
cli.add_command(load.run)
cli.add_command(load_metadata.run)
cli.add_command(publish)
cli.add_command(shell)
cli.add_command(transform.list)
cli.add_command(transform.run)
cli.add_command(transform.reverse)
cli.add_command(validate.list)
cli.add_command(validate.run)

# Initialize prod Mongo connection
try:
    init_db()
except ConnectionError:
    pass
