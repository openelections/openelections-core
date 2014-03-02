## Open Elections core code

Core repo for election results data acquisition, transformation and output.

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

Add the `openelex` directory to your `$PYTHONPATH`, so that `invoke` can see our tasks. Paste the following line into `.bashrc` or `.bash_profile` or whatever your shell loads on login:

```bash
export PYTHONPATH=$PYTHONPATH:`pwd`/openelex
```

Create `settings.py` from the template

```bash
$ cp settings.py.tmplt openelex/settings.py
```

#### Configuring services (optional)

`openelex/settings.py` can be configured for MongoDB and AWS. You only need to set these configs if you plan to archive files on your own S3 account, or write data loaders.

To configure S3 to cache your raw results, update these values in `openelex/settings.py`:

```python
AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY =''
```

To store your data in MongoDB, you need only [install Mongo](http://docs.mongodb.org/manual/installation/). The [default configuration](https://github.com/openelections/core/blob/master/settings.py.tmplt#L5-L18) should auto-create the databases and tables you need, as you need them.