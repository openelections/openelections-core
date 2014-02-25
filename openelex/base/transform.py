from collections import OrderedDict

from .state import StateBase


class Transform(object):
    """Wrapper for transform function and its validators"""

    def __init__(self, func, validators=[]):
        self._func = func
        self._validators = OrderedDict()

    def add_validation(self, *validators):
        """
        Associate validation functions with this transform.
        """
        for v in validators:
            self._validators[v.__name__] = v

    @property
    def validators(self):
        return self._validators.values()

    @property
    def name(self):
        return self._func.__name__

    def __str__(self):
        return self.name

    def __call__(self):
        self._func()
    run = __call__

    
class Registry(StateBase):

    _registry = {}

    def register(self, state, transform, validators=[]):
        try:
            state_xforms = self._registry[state]
        except KeyError:
            self._registry[state] = OrderedDict()
            state_xforms = self._registry[state]

        if isinstance(transform, Transform):
            transform_obj = transform
        else:
            transform_obj = Transform(transform)

        transform_obj.add_validation(*validators)
        state_xforms[transform_obj.name] = transform_obj 

    def get(self, state, func_name):
        try:
            transform = self._registry[state][func_name]
        except KeyError:
            err_msg = "Transform (%s) not registered for %s" % (func_name, state)
            raise KeyError(err_msg)
        return transform

    def all(self, state):
        return self._registry[state].values()


# Global object for registering transform functions
registry = Registry()
