from invoke import task

import code

@task
def shell():
    """
    Open a Python shell, bootstrapping the connection to MongoDB
    """
    code.interact()
