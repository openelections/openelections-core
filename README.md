## Open Elections core code

Core repo for election results data acquisition, transformation and output.

OpenElections core is developed and tested using Python 2.7.\*. The package
might not work with older or newer Python distributions.

### Getting started as a developer

You'll:

* set up a virtual environment
* fork/clone this repository, install dependencies
* add any optional configuration details you need (e.g. Mongo or AWS)

#### Setting up a virtual environment

You should use [virtualenv](http://www.virtualenv.org/en/latest/) and [virtualenvwrapper](http://virtualenvwrapper.readthedocs.org/) to work on Open Elections inside a virtualized development environment.

The easiest way is to install these tools system-wide with `pip` (you may need to use `sudo`):

```bash
$ pip install virtualenv
$ pip install virtualenvwrapper
```

Then, to make a virtual environment called `openelex` for open elections work:

```bash
$ mkvirtualenv openelex
```

This will automatically activate the `openelex` environment. To turn it on for future sessions:

```bash
$ workon openelex
```

#### Fork and set up this project

[Fork this repo](https://help.github.com/articles/fork-a-repo) by hitting the "Fork" button above, and clone your fork to your computer:

```bash
$ git clone git@github.com:[my_github_user]/core.git openelex-core
$ cd openelex-core
```

Turn on your virtual environment from the previous step, if you haven't already:

```bash
$ workon openelex
```

Then install the Python dependencies:

```bash
$ pip install -r requirements.txt
$ pip install -r requirements-dev.txt
```

Create `settings.py` from the template

```bash
$ cp settings.py.tmplt openelex/settings.py
```

#### Setting up 'invoke'

OpenElections uses [invoke](http://docs.pyinvoke.org/en/latest/) to run tasks (similar to Ruby's `rake`).

First, make sure you're in the **root of the repository** you've cloned.

Add the `openelex` directory to your `$PYTHONPATH`, so that `invoke` can see our tasks. This will append to your shell's login script (replace `.bashrc` with whatever your shell uses, if needed).

```bash
echo "export PYTHONPATH=$PYTHONPATH:`pwd`/openelex" >> ~/.bashrc
```

That will run automatically for future terminal sessions. To activate it for the current session:

```bash
source ~/.bashrc
```

All `invoke` commands must be run **from the project root**.

Test it out by running `invoke --list`, you should see something like:

```bash
$ invoke --list
Available tasks:

    fetch
    archive.delete
    archive.save
    cache.clear
    cache.files
    datasource.elections
    datasource.filename_url_pairs
    datasource.mappings
    datasource.target_urls
    load.run
    transform.list
    transform.run
    validate.list
    validate.run
```

Running commands looks something like this:

```bash
$ invoke cache.clear --state=NY
0 files deleted
0 files still in cache
```

You can also get help on particular commands, e.g. `invoke --help cache.clear`.

#### Configuring services (optional)

`openelex/settings.py` can be configured for MongoDB and AWS. You only need to set these configs if you plan to archive files on your own S3 account, or write data loaders.

To configure S3 to cache your raw results, update these values in `openelex/settings.py`:

```python
AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY =''
```

#### Install MongoDB (optional)

You only need to install MongoDB if you plan to write data loaders.

To store your data in MongoDB, you need only [install Mongo](http://docs.mongodb.org/manual/installation/). The [default configuration](https://github.com/openelections/core/blob/master/settings.py.tmplt#L5-L18) should auto-create the databases and tables you need, as you need them.

#### Load party and office metadata (optional)

You only need to do this if you plan to write data loaders or transforms.

```bash
$ cd openelex
$ invoke load_metadata.run --collection=office
$ invoke load_metadata.run --collection=party
```
