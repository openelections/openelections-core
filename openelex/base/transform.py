import os
import re
import sys

# Hack to avoid having our own us module shadown python-us module of same name
sys.path.pop(0)
import us


class Registry(object):

    def __init__(self):
        self._registry = {}

    def register(self, state, func, group='all'):
        try:
            state_xforms = self._registry[state]
        except KeyError:
            self._registry[state] = {}
            state_xforms = self._registry[state]
        state_xforms.setdefault(group, []).append(func)

    def get(self, state, group='all'):
        try:
            xforms = self._registry[state][group]
        except KeyError:
            err_msg = "No transforms registered for %s and %s group" % (state, group)
            raise KeyError(err_msg)
        return xforms

    def available(self, state):
        return self._registry[state]

# Global object for registering transform functions
registry = Registry()
