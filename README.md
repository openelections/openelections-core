core
====

Core repo for election results data acquisition, transformation and output.

Getting started as a developer
------------------------------

Create a sandboxed development environment using [virtualenv](http://www.virtualenv.org/en/latest/)
```bash
# cd to your virtualenv home
cd ~/.virtualenvs
$ virtualenv openelex-core
```

Jump in and activate your virtualenv
```bash
$ cd openelex-core
$ . bin/activate

```

NOTE: To deactivate your environment:
```bash
$ deactivate
```

[Fork](https://help.github.com/articles/fork-a-repo) and clone the openelex-core repo on Github
to wherever you stash your code.
```bash
# 
$ mkdir src/
$ cd src/
$ git clone git@github.com:<my_github_user>/core.git openelex-core
$ cd openelex-core 
```

Install the python dependencies
**Perform below commands while inside an active virtual environment**
```bash
$ pip install -r requirements.txt
$ pip install -r requirements-dev.txt
```

Add the ``openelex`` package to your PYTHONPATH
```bash
$ export PYTHONPATH=$PYTHONPATH:`pwd`
```

Create ``settings.py`` from the template 
```bash
$ cp settings.py.tmplt openelex/settings.py
```

(Optional) Edit settings.py to add AWS configs and db configs
You only need to set these configs if you plan to
archive files on your own S3 account or write data loaders.
```bash
$ vim openelex/settings.py
```

(Optional) [Install mongo](http://docs.mongodb.org/manual/installation/)
You only need to install mongo if you plan to write data loaders.
