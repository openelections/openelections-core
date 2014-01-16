import os
import re
import sys
from collections import OrderedDict

from .state import StateBase


class Registry(StateBase):

    _registry = {}

    def register(self, state, func):
        try:
            state_xforms = self._registry[state]
        except KeyError:
            self._registry[state] = OrderedDict()
            state_xforms = self._registry[state]
        state_xforms[func.func_name] = func

    def get(self, state, func_name):
        try:
            transform = self._registry[state][func_name]
        except KeyError:
            err_msg = "Transform (%s) not registered for %s" % (func_name, state)
            raise KeyError(err_msg)
        return transform

    def all(self, state):
        return self._registry[state]

# Global object for registering transform functions
registry = Registry()
