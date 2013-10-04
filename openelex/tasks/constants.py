import os
from os.path import dirname, join

AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')

PROJECT_ROOT = dirname(dirname(__file__))
PKG_DIR = join(PROJECT_ROOT, 'openelex')
COUNTRY_DIR = join(PKG_DIR, 'us')
