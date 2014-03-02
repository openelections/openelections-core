#!/usr/bin/env python

from distutils.core import setup

setup(
    name='Distutils',
    version='0.1.0',
    author='OpenElections',
    author_email='openelections@gmail.com',
    url='http://openelections.net',
    packages=['distutils', 'distutils.command'],
    scripts=['scripts/manage.py'],
)
