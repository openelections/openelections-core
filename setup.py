#!/usr/bin/env python

import sys
from setuptools import setup, find_packages

# TODO: More informative message about Python version support, version reflected
# classifiers argument to setup(). Perhaps setup should fail altogether for
# unsupported Python versions.
if sys.version_info < (2,7) or sys.version_info >= (3,):
    print("This package is primarily developed and tested using Python 2.7.*. "
          "It may not not work with older or newer Python distributions.")

setup(
    name='OpenElections Core',
    version='0.1.0',
    author='OpenElections',
    author_email='openelections@gmail.com',
    url='http://openelections.net',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'beautifulsoup4==4.6.0',
        'blinker==1.3',
        'ipython==0.13.2',
        'mongoengine==0.9.0',
        'nameparser==0.5.3',
        'ordered-set==2.0.2',
        'pymongo==2.8',
        'requests>=2.0',
        'unicodecsv==0.12.0',
        'us==1.0.0',
        'xlrd==0.9.2',
        'github3.py==0.9.0',
        'click==3.3',
        'Clarify==0.3',
        'funcsigs==1.0.2',
        'ipaddress==1.0.18',
        'future==0.16.0',
    ],
    tests_require=[
        'mock==1.0.1',
        'nose==1.3.0',
        'factory_boy==2.4.1',
    ],
    entry_points={
        'console_scripts': [
            'openelex = openelex.tasks:cli',
        ],
    }
)
