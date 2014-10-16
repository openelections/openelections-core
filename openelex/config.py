"""Manage configuration for various OpenElections components"""

import imp
import os
from importlib import import_module

class Settings(object):
    """
    Encapsulate settings and provide utilities for adding settings from
    elsewhere.

    The design of this class is heavily influenced by how the Django and Flask
    frameworks handle configuration.

    """
    def from_object(self, obj):
        """Copy uppercase attributes from another object to this one"""
        for key in dir(obj):
            if key.isupper():
                val = getattr(obj, key)
                setattr(self, key, val)

        return self

    def from_module_name(self, name):
        """
        Load settings attributes from a Python module

        Args:
            name (str): The name of a Python module.

        """
        module = import_module(name)
        return self.from_object(module)

    def from_file(self, filename):
        """
        Load settings from a Python file

        Args:
            filename (str): Absolute path to a Python file where settings
                variables are defined.
        """
        config_mod = imp.new_module('config')
        execfile(filename, config_mod.__dict__)
        return self.from_object(config_mod)

    def from_envvar(self, name):
        """
        Load settings from a Python file whose filename is in an environment
        variable

        Args:
            name (str): Environment variable containing the absolute path to
                a Python file where settings variables are defined.
        """
        return self.from_file(os.environ[name])


settings = Settings()
try:
    # Add settings from the old openelex.settings module to not break the
    # environment of contributors who followed the original setup instructions.
    # At some point, we should stop supporting this.
    # TODO: Deprecate this
    settings.from_module_name('openelex.settings')
except ImportError:
    pass

try:
    settings.from_envvar('OPENELEX_SETTINGS')
except KeyError:
    print("The environment variable OPENELEX_SETTINGS has not been set.  You "
          "should set this environment variable as the absolute path to a "
          "Python file containing your settings.")
