from openelex.config import settings
from mongoengine import connect

def init_db(name='openelex'):
    return connect(name, **settings.MONGO[name])[name]
