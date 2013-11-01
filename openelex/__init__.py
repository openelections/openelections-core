import os
from os.path import dirname, join

AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')

PROJECT_ROOT = dirname(__file__)
COUNTRY_DIR = join(PROJECT_ROOT, 'us')
