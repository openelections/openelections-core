## Open Elections core code

Core repo for election results data acquisition, transformation and output.

OpenElections core is developed and tested using Python 2.7.*. The package
might not work with older or newer Python distributions.

### Getting started as a developer

You'll:

* set up a virtual environment
* fork/clone this repository, install dependencies
* add any optional configuration details you need (e.g. Mongo or AWS)

#### Setting up a virtual environment

You should use [pipenv](https://pipenv.readthedocs.io/en/latest/) to work on Open Elections inside a virtualized development environment.

The easiest way is to install these tools system-wide with `pip` (you may need to use `sudo`):

```bash
$ pip install --upgrade pipenv
```

Then, to make a virtual environment for open elections work:

```bash
$ pipenv install --dev
```

To activate the virtual environment, run:

```bash
$ pipenv shell
```

#### Fork and set up this project

[Fork this repo](https://help.github.com/articles/fork-a-repo) by hitting the "Fork" button above, and clone your fork to your computer:

```bash
$ git clone https://github.com/[my_github_user]/openelections-core.git
$ cd openelections-core
```

Turn setup and activate the virtual environment from the previous step, if you haven't already:

```bash
$ pipenv install --python 2.7 --dev
$ pipenv shell
```

Create a `settings.py` file.

```bash
$ cp settings.py.tmplt settings.py
```

At the very least, you'll want to make sure the values in the ``MONGO`` variable work for the way you've installed and configured MongoDB on your system.

You can put this settings file anywhere on your filesystem.  You'll need to set the ``OPENELEX_SETTINGS`` environment variable to the [absolute path](http://en.wikipedia.org/wiki/Path_(computing)) to the ``settings.py`` file that you created.

You can set the `OPENELEX_SETTINGS` environment variable by creating a `.env` file in your repository. For example:

```
OPENELEX_SETTINGS=/Users/[myusername]/Development/openelections-core/settings.py
```

Running `pipenv shell` will automatically source any variable in your `.env` file.

#### Running management commands

Test it out by running `openelex --help`, you should see something like:

```bash
$ openelex --help
Usage: openelex [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  bake.election_file             Write election and candidate data with on...
  bake.results_status_json       Output a JSON file describing available...
  bake.state_file                Write election and candidate data along
                                 with...
  cache.clear                    Delete files in state cache diretory
  cache.files                    List files in state cache diretory
  datasource.elections           List elections for a state.
  datasource.filename_url_pairs  List mapping of standard filenames to
                                 source...
  datasource.mappings            List metadata mappings for a state
  datasource.target_urls         List source data urls for a state
  fetch                          Scrape data files and store in local file...
  load.run                       Load cached data files into the database
  load_metadata.run              Populate metadata in database from fixture...
  publish                        Publish baked result files
  shell                          Open a Python shell, bootstrapping the...
  transform.list                 Show available data transformations
  transform.reverse              Reverse a previously run transformation
  transform.run                  Run data transformations
  validate.list                  Show available validations for state
  validate.run                   Run data validations for state
```

Running commands looks something like this:

```bash
$ openelex cache.clear --state=NY
0 files deleted
0 files still in cache
```

You can also get help on particular commands, e.g. `openelex --help cache.clear`.

#### Configuring services (optional)

If you are going to load results, you need to configure MongoDB connections in
`openelex/settings.py`.

If you are a core contributor who needs to publish baked results to one of the GitHub repositories, you will need to define further settings.

To set GitHub credentials, you must first create a [personal access token](https://help.github.com/articles/creating-an-access-token-for-command-line-use) and then uncomment and set these values in `openelex/settings.py`:

```python
GITHUB_USERNAME = ''
GITHUB_ACCESS_TOKEN = ''
```

#### Install MongoDB (optional)

You only need to install MongoDB if you plan to write data loaders.

To store your data in MongoDB, you need only [install Mongo](http://docs.mongodb.org/manual/installation/). The [default configuration](https://github.com/openelections/openelections-core/blob/master/settings.py.tmplt#L7-L20) should auto-create the databases and tables you need, as you need them.

#### Load party and office metadata (optional)

You only need to do this if you plan to write data loaders or transforms.

```bash
$ cd openelex
$ openelex load_metadata.run --collection=office
$ openelex load_metadata.run --collection=party
```
